import os
import shutil
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from workflows.flows.data_dumps.ebi_search.utils.checkpoints import (
    write_last_dump_date,
)
from workflows.prefect_utils.flows_utils import django_db_task


@django_db_task(name="Publish EBI Search dump")
def publish_dump(source: Path, destination: Path, until: datetime) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup_path = destination.parent / f".{destination.name}.{uuid4().hex}.old"
    try:
        if destination.exists():
            os.replace(destination, backup_path)
        os.replace(source, destination)
    except Exception:
        if backup_path.exists() and not destination.exists():
            os.replace(backup_path, destination)
        raise
    if backup_path.exists():
        shutil.rmtree(backup_path)
    write_last_dump_date(until)
    return str(destination)
