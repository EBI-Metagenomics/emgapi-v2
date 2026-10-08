from datetime import datetime, timezone
from pathlib import Path

from django.db import connections
from psycopg.conninfo import make_conninfo

from activate_django_first import EMG_CONFIG

from proteins.dims import resolve
from proteins.load import load
from proteins.owner import owner
from workflows.prefect_utils.flows_utils import django_db_flow as flow


def proteindb_dsn() -> str:
    """The proteindb alias as a libpq connection string, for psycopg and ADBC."""
    database = connections["proteindb"].settings_dict
    return make_conninfo(
        dbname=database["NAME"],
        **{
            setting.lower(): database[setting]
            for setting in ("HOST", "PORT", "USER", "PASSWORD")
            if database[setting]
        },
        **database["OPTIONS"],
    )


@flow(flow_run_name="Load the protein DB's staged rows into Tier 2")
def proteindb_load():
    """Moves everything staged in Tier 1 into a new day of Tier 2, and compacts Tier 2 when due."""
    dsn = proteindb_dsn()
    config = EMG_CONFIG.proteindb
    with owner(dsn, "load", config.tier2_concurrency_limit) as taken:
        load(
            dsn,
            Path(config.root) / "tier2",
            datetime.now(timezone.utc).date(),
            resolve,
            taken,
        )
