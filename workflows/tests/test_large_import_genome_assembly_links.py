import pytest
from prefect import flow

from workflows.flows.import_genome_assembly_links_flow import read_tsv_file
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.django_db
@pytest.mark.use_large_genome_links_tsv
def test_read_tsv_file_one_million_records(tmp_path):  # ← inject tmp_path
    """
    Generate a TSV file with 1,000,000 records and ensure read_tsv_file can read them.
    This test is opt-in because it is memory/time intensive. Enable with: pytest -m large_tsv
    """
    total = 1_000_000
    file_path = tmp_path / "large.tsv"

    batch = []
    batch_size = 10_000
    with file_path.open("w", encoding="utf-8", newline="") as f:
        f.write("primary_assembly\tgenome\n")
        for i in range(1, total + 1):
            batch.append(f"ERZ{i:06d}\tMGYG{i:09d}\n")
            if len(batch) >= batch_size:
                f.writelines(batch)
                batch.clear()
        if batch:
            f.writelines(batch)

    # Wrap the task call in a Prefect flow (consistent with other tests)
    @flow(name="Test Read TSV File - One Million Records")
    def test_flow(file_path: str):
        return read_tsv_file(file_path)

    records = run_flow_and_capture_logs(test_flow, str(file_path)).result
    assert len(records) == total
    # Spot-check a couple of values
    assert records[0]["primary_assembly"] == "ERZ000001"
    assert records[0]["genome"] == "MGYG000000001"
    assert records[-1]["genome"] == "MGYG001000000"
