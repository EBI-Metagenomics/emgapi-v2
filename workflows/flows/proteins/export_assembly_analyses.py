from pathlib import Path

import pyarrow.parquet as pq
from prefect import get_run_logger

from activate_django_first import EMG_CONFIG  # noqa: F401

from proteins.dataset import metadata
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


@task(task_run_name="Read the assembly analyses")
def read_assembly_analyses():
    return metadata.export_assembly_analyses()


@flow(flow_run_name="Export the assembly analyses to {out}")
def export_assembly_analyses(out: str) -> int:
    """Writes every assembly analysis, by (ERZ accession, major.minor), with its MGYA, study, biome and flags, to a Parquet file. Run by hand.

    For work outside emgapi-v2 that must give an MGYA to an assembly. The legacy protein import
    reads this file.

    :param out: The Parquet file to write.
    :return: The number of rows.
    """
    logger = get_run_logger()
    table = read_assembly_analyses()
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    pq.write_table(table, tmp)
    tmp.rename(path)
    logger.info(f"Wrote {table.num_rows} assembly analyses to {path}")
    return table.num_rows
