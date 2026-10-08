import hashlib
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from proteins import tier2
from proteins.tier2 import Tier2Error, files, prefix, write

D1, D2, D3, D4, D5 = (date(2026, 10, d) for d in range(1, 6))


def touch(root, *paths):
    for path in paths:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).touch()


def complete(root, *days):
    for day in days:
        (root / "dims" / f"snapshot={day}").mkdir(parents=True)


def relative(root, paths):
    return [str(path.relative_to(root)) for path in paths]


def test_prefix_is_the_top_six_bits_in_hex():
    assert [prefix(bytes([b]) + bytes(31)) for b in (0, 3, 4, 0xFB, 0xFC, 0xFF)] == [
        "00",
        "00",
        "01",
        "3e",
        "3f",
        "3f",
    ]


def test_protein_is_the_newest_base_and_the_parts_after_it(tmp_path):
    complete(tmp_path, D1, D2, D3, D4)
    touch(
        tmp_path,
        f"protein/prefix=00/base-{D1}.parquet",
        f"protein/prefix=00/part-{D2}.parquet",
        f"protein/prefix=00/base-{D3}.parquet",
        f"protein/prefix=00/part-{D4}.parquet",
        f"protein/prefix=01/base-{D1}.parquet",
        f"protein/prefix=01/part-{D2}.parquet",
        f"protein/prefix=01/part-{D3}.parquet",
    )
    assert relative(tmp_path, files(tmp_path, "protein")) == [
        f"protein/prefix=00/base-{D3}.parquet",
        f"protein/prefix=00/part-{D4}.parquet",
        f"protein/prefix=01/base-{D1}.parquet",
        f"protein/prefix=01/part-{D2}.parquet",
        f"protein/prefix=01/part-{D3}.parquet",
    ]
    assert relative(tmp_path, files(tmp_path, "protein", as_of=D2)) == [
        f"protein/prefix=00/base-{D1}.parquet",
        f"protein/prefix=00/part-{D2}.parquet",
        f"protein/prefix=01/base-{D1}.parquet",
        f"protein/prefix=01/part-{D2}.parquet",
    ]


def test_a_prefix_without_a_base_is_its_parts(tmp_path):
    complete(tmp_path, D1, D2)
    touch(tmp_path, f"protein/prefix=3f/part-{D1}.parquet")
    assert relative(tmp_path, files(tmp_path, "protein")) == [
        f"protein/prefix=3f/part-{D1}.parquet"
    ]


def test_files_of_incomplete_days_are_never_read(tmp_path):
    complete(tmp_path, D1, D3)
    touch(
        tmp_path,
        f"protein/prefix=00/base-{D1}.parquet",
        f"protein/prefix=00/part-{D2}.parquet",
        f"protein/prefix=00/part-{D3}.parquet",
        f"protein/prefix=00/part-{D4}.parquet",
        f"protein/prefix=00/.part-{D3}.parquet.tmp",
        f"occurrence/ingest_date={D1}/part-000.parquet",
        f"occurrence/ingest_date={D2}/part-000.parquet",
        f"occurrence/ingest_date={D3}/part-000.parquet",
        f"occurrence/ingest_date={D3}/.part-001.parquet.tmp",
        f"occurrence/ingest_date={D4}/part-000.parquet",
        f"dims/.tmp-snapshot={D4}/assembly.parquet",
    )
    assert relative(tmp_path, files(tmp_path, "protein")) == [
        f"protein/prefix=00/base-{D1}.parquet",
        f"protein/prefix=00/part-{D3}.parquet",
    ]
    assert relative(tmp_path, files(tmp_path, "occurrence")) == [
        f"occurrence/ingest_date={D1}/part-000.parquet",
        f"occurrence/ingest_date={D3}/part-000.parquet",
    ]
    with pytest.raises(Tier2Error, match="not a complete day"):
        files(tmp_path, "protein", as_of=D4)


def test_occurrence_and_contig_are_every_day_up_to_as_of(tmp_path):
    complete(tmp_path, D1, D2, D3)
    touch(
        tmp_path,
        *(f"contig/ingest_date={D1}/part-{n:03d}.parquet" for n in (1, 0, 63)),
        f"contig/ingest_date={D2}/part-000.parquet",
        f"contig/ingest_date={D3}/part-000.parquet",
    )
    assert relative(tmp_path, files(tmp_path, "contig", as_of=D2)) == [
        f"contig/ingest_date={D1}/part-000.parquet",
        f"contig/ingest_date={D1}/part-001.parquet",
        f"contig/ingest_date={D1}/part-063.parquet",
        f"contig/ingest_date={D2}/part-000.parquet",
    ]


def test_dimensions_are_the_snapshot_of_as_of(tmp_path):
    complete(tmp_path, D1, D2)
    assert relative(tmp_path, files(tmp_path, "dims/biome")) == [
        f"dims/snapshot={D2}/biome.parquet"
    ]
    assert relative(tmp_path, files(tmp_path, "dims/assembly", as_of=D1)) == [
        f"dims/snapshot={D1}/assembly.parquet"
    ]


def test_as_of_older_than_the_oldest_base_raises(tmp_path):
    complete(tmp_path, D1, D2, D5)
    touch(
        tmp_path,
        f"protein/prefix=00/base-{D1}.parquet",
        f"protein/prefix=01/base-{D5}.parquet",
    )
    with pytest.raises(Tier2Error, match="prefix=01"):
        files(tmp_path, "protein", as_of=D2)


def test_no_complete_day_raises(tmp_path):
    touch(tmp_path, f"occurrence/ingest_date={D1}/part-000.parquet")
    with pytest.raises(Tier2Error, match="no complete day"):
        files(tmp_path, "occurrence")


def test_unknown_table_raises(tmp_path):
    with pytest.raises(ValueError, match="unknown"):
        files(tmp_path, "proteins")


def hash_of(n):
    return hashlib.sha256(str(n).encode()).digest()


def test_write_sorts_casts_and_leaves_no_temporary_file(tmp_path):
    rows = 2 * tier2.ROW_GROUP_SIZE + 1
    path = tmp_path / "protein" / "prefix=00" / f"part-{D1}.parquet"
    write(
        path,
        "protein",
        pa.table(
            {
                "sequence": [f"M{n}" for n in range(rows)],
                "id": list(range(rows)),
                "hash": [hash_of(n) for n in range(rows)],
            }
        ),
    )

    assert [p.name for p in path.parent.iterdir()] == [path.name]
    stored = pq.read_table(path)
    assert stored.schema == tier2.SCHEMAS["protein"]
    assert stored["hash"].to_pylist() == sorted(hash_of(n) for n in range(rows))
    metadata = pq.ParquetFile(path).metadata
    assert [metadata.row_group(g).num_rows for g in range(metadata.num_row_groups)] == [
        tier2.ROW_GROUP_SIZE,
        tier2.ROW_GROUP_SIZE,
        1,
    ]
    column = metadata.row_group(0).column(1)
    assert column.compression == "ZSTD"
    assert column.statistics.has_min_max


def test_write_sorts_occurrences_by_assembly_contig_and_start(tmp_path):
    path = tmp_path / "occurrence.parquet"
    keys = [(2, 1, 5), (1, 2, 1), (1, 1, 9), (1, 1, 3)]
    write(
        path,
        "occurrence",
        pa.table(
            {
                "id": range(4),
                "protein_id": range(4),
                "contig_id": [c for _, c, _ in keys],
                "assembly_id": [a for a, _, _ in keys],
                "gene_caller_id": [1] * 4,
                "start_position": [s for _, _, s in keys],
                "end_position": [10] * 4,
                "strand": [1, -1, 1, -1],
                "truncation": ["00", None, "11", "01"],
            }
        ),
    )
    stored = pq.read_table(path)
    assert list(
        zip(
            stored["assembly_id"].to_pylist(),
            stored["contig_id"].to_pylist(),
            stored["start_position"].to_pylist(),
        )
    ) == sorted(keys)
    assert stored.schema == tier2.SCHEMAS["occurrence"]


def test_write_rejects_null_in_a_required_column_and_writes_nothing(tmp_path):
    path = tmp_path / "biome.parquet"
    with pytest.raises(ValueError):
        write(path, "dims/biome", pa.table({"id": [1, 2], "lineage": ["root", None]}))
    assert list(tmp_path.iterdir()) == []


def test_write_replaces_an_existing_file(tmp_path):
    path = tmp_path / "biome.parquet"
    write(path, "dims/biome", pa.table({"id": [1], "lineage": ["root"]}))
    write(path, "dims/biome", pa.table({"id": [2], "lineage": ["root:Host"]}))
    assert pq.read_table(path).to_pylist() == [{"id": 2, "lineage": "root:Host"}]
    assert [p.name for p in tmp_path.iterdir()] == ["biome.parquet"]
