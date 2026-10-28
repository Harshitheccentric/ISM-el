"""Where each quarter's zip comes from.

A quarter's URL is either listed in docs/data-sources.md, or derived from the same base
URL as data_QN_YYYY.zip and accepted only after a successful HEAD request. A quarter
whose derived URL fails HEAD is "missing": it is logged and skipped, never fatal.

The sources stage writes sources/sources.csv: one row per quarter in the config range
with its url and kind (listed | derived | missing). The status table shows it.
"""

import csv
import http.client
import re
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

from diskctx.config import quarters_between
from diskctx.status import SOURCES_FILE
from diskctx.storage import DURABLE, Stage

DOC_PATH = Path(__file__).resolve().parents[2] / "docs" / "data-sources.md"

URL_RE = re.compile(r"https?://\S+?/data_Q([1-4])_(\d{4})\.zip")
COLUMNS = ["quarter", "url", "kind", "data_source"]
DOWNLOAD_TRIES = 5        # per run; a rerun resumes from the partial file
RETRY_WAIT_SECONDS = 30   # wait 30, 60, 90, 120 s between tries, for short outages


class SourceError(RuntimeError):
    pass


class DownloadError(SourceError):
    pass


def listed_urls(doc_path=DOC_PATH):
    """{quarter: url} for every quarter zip URL written in docs/data-sources.md."""
    listed = {}
    for m in URL_RE.finditer(Path(doc_path).read_text()):
        listed[f"{m.group(2)}Q{m.group(1)}"] = m.group(0)
    if not listed:
        raise SourceError(f"no quarter zip URLs found in {doc_path}")
    return listed


def base_url(listed):
    """The folder URL shared by all listed zips. Refuses to guess if they differ."""
    bases = {url.rsplit("/", 1)[0] for url in listed.values()}
    if len(bases) != 1:
        raise SourceError(f"listed URLs do not share one base, cannot derive others: {sorted(bases)}")
    return bases.pop()


def derived_url(base, quarter):
    """'2016Q1' -> <base>/data_Q1_2016.zip"""
    year, q = quarter.split("Q")
    return f"{base}/data_Q{q}_{year}.zip"


def quarter_of(url):
    """'.../data_Q1_2026.zip' -> '2026Q1'"""
    m = URL_RE.search(url)
    return f"{m.group(2)}Q{m.group(1)}"


def head_ok(url, timeout=30):
    """True if a HEAD request for url succeeds (HTTP 200)."""
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status == 200
    except Exception:  # HTTP 404, DNS failure, timeout: the quarter is missing
        return False


def resolve_sources(quarters, listed, head=head_ok):
    """One row per quarter: listed, derived (HEAD succeeded) or missing (HEAD failed)."""
    base = base_url(listed)
    rows = []
    for quarter in quarters:
        if quarter in listed:
            url, kind = listed[quarter], "listed"
        else:
            url = derived_url(base, quarter)
            kind = "derived" if head(url) else "missing"
        print(f"source {quarter}: {kind} {url}")
        rows.append({"quarter": quarter, "url": url, "kind": kind})
    return rows


def sources_stage(cfg, head=None, doc_path=DOC_PATH):
    """The sources stage. Reruns when the range or the listed URLs change.
    head: HEAD check for derived URLs (default: head_ok)."""
    listed = listed_urls(doc_path)
    quarters = quarters_between(cfg["start_quarter"], cfg["end_quarter"])

    def build(work, storage):
        rows = resolve_sources(quarters, listed, head or head_ok)
        out = work / SOURCES_FILE
        out.parent.mkdir(parents=True)
        with open(out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({**row, "data_source": storage.data_source})

    return Stage(
        name="sources",
        tier=DURABLE,
        outputs=[SOURCES_FILE],
        build=build,
        keys=["start_quarter", "end_quarter"],
        params={"listed": listed},
    )


def read_sources(storage):
    """Rows of sources/sources.csv, in quarter order."""
    with open(storage.path(DURABLE, SOURCES_FILE), newline="") as f:
        return list(csv.DictReader(f))


def total_size(response, resumed):
    """The whole file's size in bytes: from Content-Range when resuming ('bytes 300-999/1000'),
    else from Content-Length. None if the server does not say."""
    if resumed:
        content_range = response.headers.get("Content-Range", "")
        return int(content_range.rsplit("/", 1)[1]) if "/" in content_range else None
    length = response.headers.get("Content-Length")
    return int(length) if length is not None else None


def download(url, dest, tries=DOWNLOAD_TRIES, wait=RETRY_WAIT_SECONDS):
    """Download url to dest, resuming after network drops.

    Bytes go to dest.part. If dest.part already holds the start of the file (an earlier
    try or an earlier run), only the rest is requested (an HTTP Range request); if the
    server ignores the range, it starts over. The file gets its final name only when its
    size matches the server's. A failed try waits (wait, 2 x wait, ...) and tries again;
    after `tries` it raises DownloadError and keeps dest.part, so the next run resumes.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, tries + 1):
        have = part.stat().st_size if part.exists() else 0
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
                resumed = have > 0 and response.status == 206   # 200 = range ignored: start over
                total = total_size(response, resumed)
                with open(part, "ab" if resumed else "wb") as f:
                    shutil.copyfileobj(response, f, length=1 << 20)
            got = part.stat().st_size
            if total is None or got == total:
                part.replace(dest)
                return
            if got > total:
                part.unlink()           # more bytes than the file has: not trustworthy
            problem = f"got {got} of {total} bytes"
        except urllib.error.HTTPError as e:
            if e.code == 416:           # the range starts past the end: start over
                part.unlink(missing_ok=True)
            problem = f"HTTP {e.code}"
        except (OSError, http.client.HTTPException) as e:  # timeouts, resets, DNS
            problem = f"{type(e).__name__}: {e}"
        print(f"download {url}: {problem} (try {attempt} of {tries})", flush=True)
        if attempt < tries:
            time.sleep(wait * attempt)
    raise DownloadError(f"could not download {url} after {tries} tries ({problem}); "
                        f"rerun to resume from the partial file")


def local_zip(storage, url, fetch=None):
    """The quarter zip on local disk: reused if a readable zip is there, else fetched
    with fetch(url, dest) (default: download). A file that is not a readable zip (e.g.
    left by an older, cut-short download) is deleted and fetched again."""
    dest = storage.download_dir / url.rsplit("/", 1)[1]
    if dest.exists() and not zipfile.is_zipfile(dest):
        print(f"{dest.name} is not a readable zip; downloading it again", flush=True)
        dest.unlink()
    if not dest.exists():
        (fetch or download)(url, dest)
        if not zipfile.is_zipfile(dest):
            dest.unlink()
            raise DownloadError(f"{url} did not give a readable zip; rerun to try again")
    return dest
