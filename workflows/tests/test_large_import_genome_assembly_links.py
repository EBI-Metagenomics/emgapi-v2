import os
import tempfile

import pytest
from prefect import flow

from workflows.flows.import_genome_assembly_links_flow import read_tsv_file
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.django_db
@pytest.mark.skipif(
    not os.getenv("RUN_LARGE_TSV_TESTS"),
    reason="Large TSV test is skipped by default. Set RUN_LARGE_TSV_TESTS=1 to enable.",
)
def test_read_tsv_file_one_million_records():
    """
    Generate a TSV file with 1,000,000 records and ensure read_tsv_file can read them.
    This test is opt-in because it is memory/time intensive. Enable with RUN_LARGE_TSV_TESTS=1.
    """
    total = 1_000_000

    # Create a temporary TSV file efficiently using chunked writes
    temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".tsv")
    try:
        # Header with only required columns to minimise size
        temp_file.write("primary_assembly\tgenome\n")

        batch = []
        batch_size = 10000
        for i in range(1, total + 1):
            batch.append(f"ERZ{i:06d}\tMGYG{i:09d}\n")
            if len(batch) >= batch_size:
                temp_file.writelines(batch)
                batch.clear()
        if batch:
            temp_file.writelines(batch)
        temp_file.flush()
        path = temp_file.name
    finally:
        temp_file.close()

    # Wrap the task call in a Prefect flow (consistent with other tests)
    @flow(name="Test Read TSV File - One Million Records")
    def test_flow(file_path: str):
        return read_tsv_file(file_path)

    try:
        records = run_flow_and_capture_logs(test_flow, path).result
        assert len(records) == total
        # Spot-check a couple of values
        assert records[0]["primary_assembly"] == "ERZ000001"
        assert records[0]["genome"] == "MGYG000000001"  # 1 zero-padded to 9 digits
        assert records[-1]["primary_assembly"] == "ERZ1000000"  # ERZ + 7 digits
        assert (
            records[-1]["genome"] == "MGYG001000000"
        )  # 1,000,000 zero-padded to 9 digits
    finally:
        os.unlink(path)
