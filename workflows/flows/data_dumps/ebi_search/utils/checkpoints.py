from datetime import datetime

from pydantic import BaseModel

from kvstore.models import KeyValueStore

EBI_SEARCH_DUMP_STATE_KEY = "ebi_search_dump_state"


class EBISearchDumpState(BaseModel):
    last_dump_date: datetime


def read_last_dump_date() -> datetime | None:
    try:
        state = KeyValueStore.get_model(EBI_SEARCH_DUMP_STATE_KEY, EBISearchDumpState)
    except KeyValueStore.DoesNotExist:
        return None
    return state.last_dump_date


def write_last_dump_date(last_dump_date: datetime) -> None:
    KeyValueStore.set_model(
        EBI_SEARCH_DUMP_STATE_KEY,
        EBISearchDumpState(last_dump_date=last_dump_date),
    )
