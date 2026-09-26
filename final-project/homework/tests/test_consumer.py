"""Unit-тести sink-а Kafka -> landing. Kafka не потрібна: перевіряється чиста логіка запису."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from stream.consumer import landing_path, write_batch

TS = datetime(2026, 9, 21, 10, 5, tzinfo=UTC)


def test_landing_path_layout(tmp_path: Path) -> None:
    path = landing_path(tmp_path, TS, partition=2, first_offset=5, last_offset=9)
    assert path == (
        tmp_path / "dt=2026-09-21" / "hour=10" / "part-p2-o000000000005-o000000000009.ndjson"
    )


def test_file_name_encodes_the_offset_range(tmp_path: Path) -> None:
    """Той самий перший офсет, але різний останній -> різні імена.

    Інакше файл, переписаний після збою довшим, мав би те саме ім'я, Bronze (ідемпотентний за
    іменем) його пропустив би, і хвіст батча тихо загубився б.
    """
    short = landing_path(tmp_path, TS, 0, 5, 9)
    longer = landing_path(tmp_path, TS, 0, 5, 14)
    assert short != longer


def test_one_file_per_partition(tmp_path: Path) -> None:
    records = [(0, 5, b'{"a":1}'), (1, 99, b'{"b":1}'), (0, 6, b'{"a":2}')]
    written = write_batch(tmp_path, TS, records)
    assert len(written) == 2
    assert {p.name for p in written} == {
        "part-p0-o000000000005-o000000000006.ndjson",
        "part-p1-o000000000099-o000000000099.ndjson",
    }


def test_ndjson_one_object_per_line(tmp_path: Path) -> None:
    records = [(0, 1, b'{"a":1}'), (0, 2, b'{"a":2}\n')]  # у другого вже є \n
    (path,) = write_batch(tmp_path, TS, records)
    assert path.read_bytes() == b'{"a":1}\n{"a":2}\n'


def test_records_are_written_in_offset_order(tmp_path: Path) -> None:
    records = [(0, 7, b'{"n":7}'), (0, 5, b'{"n":5}'), (0, 6, b'{"n":6}')]
    (path,) = write_batch(tmp_path, TS, records)
    assert path.read_text().splitlines() == ['{"n":5}', '{"n":6}', '{"n":7}']
    assert path.name == "part-p0-o000000000005-o000000000007.ndjson"


def test_no_tmp_files_left_after_success(tmp_path: Path) -> None:
    write_batch(tmp_path, TS, [(0, 1, b"{}")])
    assert list(tmp_path.rglob("*.tmp")) == []


def test_failure_leaves_no_partial_file(tmp_path: Path) -> None:
    """Збій посеред запису не лишає ні готового файлу, ні .tmp — Spark не побачить сміття."""
    bad = [(0, 1, b'{"ok":1}'), (0, 2, "не bytes")]  # другий запис зламає write()
    with pytest.raises(TypeError):
        write_batch(tmp_path, TS, bad)  # type: ignore[arg-type]
    assert list(tmp_path.rglob("*.ndjson")) == []
    assert list(tmp_path.rglob("*.tmp")) == []


def test_rewriting_the_same_batch_is_idempotent(tmp_path: Path) -> None:
    records = [(0, 1, b'{"a":1}'), (0, 2, b'{"a":2}')]
    first = write_batch(tmp_path, TS, records)
    second = write_batch(tmp_path, TS, records)
    assert first == second
    assert len(list(tmp_path.rglob("*.ndjson"))) == 1
    assert first[0].read_text() == '{"a":1}\n{"a":2}\n'
