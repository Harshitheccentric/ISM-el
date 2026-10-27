"""Where each quarter's zip comes from.

A quarter's URL is either listed in docs/data-sources.md, or derived from the same base
URL as data_QN_YYYY.zip and accepted only after a successful HEAD request. A quarter
whose derived URL fails HEAD is "missing": it is logged and skipped, never fatal.

The sources stage writes sources/sources.csv: one row per quarter in the config range
with its url and kind (listed | derived | missing). The status table shows it.
"""

import csv
import re
import shutil
import urllib.request
from pathlib import Path

from diskctx.config import quarters_between
from diskctx.status import SOURCES_FILE
from diskctx.storage import DURABLE, Stage

DOC_PATH = Path(__file__).resolve().parents[2] / "docs" / "data-sources.md"

URL_RE = re.compile(r"https?://\S+?/data_Q([1-4])_(\d{4})\.zip")
COLUMNS = ["quarter", "url", "kind", "data_source"]


class SourceError(RuntimeError):
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


def download(url, dest):
    """Stream url to dest. Writes to dest.part first, so a broken download never looks done."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    with urllib.request.urlopen(url, timeout=60) as response, open(part, "wb") as f:
        shutil.copyfileobj(response, f, length=1 << 20)
    part.replace(dest)


def local_zip(storage, url, fetch=None):
    """The quarter zip on local disk: reused if a complete copy is there, else fetched
    with fetch(url, dest) (default: download)."""
    dest = storage.download_dir / url.rsplit("/", 1)[1]
    if not dest.exists():
        (fetch or download)(url, dest)
    return dest
