import pytest
from conftest import write_zip

from diskctx.rawzip import RawZipError, day_members, extract_member
from diskctx.synthetic import SyntheticFleet


def test_day_members_skip_junk(tmp_path):
    fleet = SyntheticFleet(1, "2026Q1", "2026Q1", n_drives=4, n_boot=1)
    names = fleet.write_quarter_zip("2026Q1", tmp_path / "q.zip")
    members = day_members(tmp_path / "q.zip")
    assert [m for _, m in members] == names                  # junk, README, dot-files skipped
    assert [d for d, _ in members] == sorted(d for d, _ in members)
    assert len(members) == 90


def test_deeply_nested_day_files_are_found(tmp_path):
    z = write_zip(tmp_path / "q.zip", {
        "a/b/c/2026-01-02.csv": "x\n",
        "x/2026-01-01.csv": "x\n",
        "__MACOSX/a/2026-01-03.csv": "junk",
        "a/.hidden/2026-01-04.csv": "junk",
        "a/._2026-01-05.csv": "junk",
        "a/2026-01-06.csv.bak": "junk",
        "a/notes.csv": "junk",
    })
    assert day_members(z) == [("2026-01-01", "x/2026-01-01.csv"), ("2026-01-02", "a/b/c/2026-01-02.csv")]


def test_duplicate_date_is_an_error(tmp_path):
    z = write_zip(tmp_path / "q.zip", {"a/2026-01-01.csv": "x\n", "b/2026-01-01.csv": "y\n"})
    with pytest.raises(RawZipError, match="appears twice"):
        day_members(z)


def test_extract_member_copies_one_day(tmp_path):
    fleet = SyntheticFleet(1, "2026Q1", "2026Q1", n_drives=4, n_boot=1)
    fleet.write_quarter_zip("2026Q1", tmp_path / "q.zip")
    day, member = day_members(tmp_path / "q.zip")[0]
    size = extract_member(tmp_path / "q.zip", member, tmp_path / "day.csv")
    expected = fleet.day_csv(fleet.days[0]).encode()
    assert (tmp_path / "day.csv").read_bytes() == expected
    assert size == len(expected)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["day.csv", "q.zip"]


def test_dropped_days_are_left_out_and_must_exist(tmp_path):
    z = write_zip(tmp_path / "q.zip", {"q/2026-01-01.csv": "a", "q/2026-01-02.csv": "b", "q/2026-01-03.csv": "c"})
    assert [d for d, _ in day_members(z, drop=["2026-01-02"])] == ["2026-01-01", "2026-01-03"]
    with pytest.raises(RawZipError, match="2026-01-09"):
        day_members(z, drop=["2026-01-09"])
