from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Union

from django.conf import settings
from django.db.models import QuerySet
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment

EMG_CONFIG = settings.EMG_CONFIG

logger = logging.getLogger(__name__)


# TODO: consider replacing this with a dataclass with a default renderer
# e.g. instead of using
#
# ```python
# class SamplesheetColumnSource(BaseModel):
#     lookup_string: str | list[str] = Field(default="id")
#     renderer: Callable[[Any], str] = Field(lambda x: x)
#     const: Any | None = Field(None)
# ```
#
# because it is a bit more readable to do
#
# ```python
#             "assembly_memory": SamplesheetColumnSource(
#                 const=memory
#             ),
#             "human_reference": SamplesheetColumnSource(
#                 const=""
#             ),
# ```
@dataclass
class SamplesheetColumnSource:
    lookup_string: str | list[str]
    renderer: Callable[[Any], str] = lambda x: x


def queryset_to_samplesheet(
    queryset: QuerySet,
    filename: Union[Path, str],
    column_map: dict[str, SamplesheetColumnSource] = None,
    bludgeon: bool = False,
) -> Path:
    """
    Write a nextflow samplesheet (TSV) where each row/sample is an object from a Django queryset.

    :param queryset: e.g. mymodel.objects.all()
    :param filename: e.g. mysamplesheet.tsv
    :param column_map: Maps columns of the sample sheet to a data source.  e.g.
        {
            "sample_id": SamplesheetColumnSource(lookup_string='id'),
            "start_time": SamplesheetColumnSource(lookup_string='my_other_model__created_at', renderer=lambda date: date[:4])
        }
    :param bludgeon: Boolean which if True will allow an existing sheet to be overwritten.
    :return: Path to the written samplesheet file.
    """

    _column_map = column_map or {}

    if not _column_map:
        for field in queryset.model._meta.get_fields():
            if not field.is_relation:
                _column_map[field.name] = SamplesheetColumnSource(
                    lookup_string=field.name
                )

    logger.info(
        f"Will write columns: {_column_map.keys()} to samplesheet at {filename}"
    )

    _filename = Path(filename)
    folder = _filename.parent
    try:
        assert folder.is_dir()
        assert folder.exists()
    except AssertionError:
        raise Exception(f"The directory {folder} does not exist")
    else:
        logger.debug(f"Samplesheet's parent folder {folder} looks okay")
    if not bludgeon:
        try:
            assert not _filename.exists()
            logger.debug(f"Samplesheet filename {_filename} doesn't exist yet (good)")
        except AssertionError:
            raise Exception(
                f"The file {_filename} already exists and bludgeon=False so will not overwrite"
            )

    delimiter = "," if _filename.suffix == ".csv" else "\t"

    with open(_filename, "w", newline="") as samplesheet:
        writer = csv.DictWriter(
            samplesheet, fieldnames=_column_map.keys(), delimiter=delimiter
        )
        writer.writeheader()
        all_lookups_requested = []
        for source in _column_map.values():
            if isinstance(source.lookup_string, list):
                all_lookups_requested.extend(source.lookup_string)
            else:
                all_lookups_requested.append(source.lookup_string)

        values_qs = queryset.values(*all_lookups_requested)
        for obj in values_qs:
            writer.writerow(
                {
                    col: (
                        source.renderer(
                            *[obj[lookup] for lookup in source.lookup_string]
                        )
                        if isinstance(source.lookup_string, list)
                        else source.renderer(obj[source.lookup_string])
                    )
                    for col, source in _column_map.items()
                }
            )

    return _filename


def queryset_hash(queryset: QuerySet, field: str) -> str:
    """
    Make a unique(ish) hash of a queryset, based on concatenating the values of one field and hashing them.
    Useful e.g. for naming a queryset-based samplesheet.

    :param queryset: e.g. mymodel.objects.filter(started_at__lt='2022')
    :param field: e.g. "accession"
    :return: A string hash of e.g. mymodel.accession values.
    """
    vals = queryset.values_list(field, flat=True)
    vals_str = "".join(map(str, vals))
    return hashlib.md5(vals_str.encode()).hexdigest()


def location_for_samplesheet_to_be_edited(
    cluster_location: Path, shared_filesystem_root: str
) -> Path:
    """
    Determine a path on the shared filesystem (an NFS locaiton available on both datamovers and k8s)
    where a samplesheet can be temporarily saved for editing.
    Parameters
    ----------
    cluster_location: e.g. /nfs/production/path/to/a/run/and/a/samplesheet123.csv
    shared_filesystem_root: e.g. /nfs/public/rw/our/dir or /app/data

    Returns
    -------
    E.g. /nfs/public/rw/our/dir/temporary_samplesheet_edits/for_editing/samplesheet123.csv
    (if samplesheet_editing_path_from_shared_filesystem config is set to `temporary_samplesheets_edits`)

    """
    samplesheet_name = Path(cluster_location).name
    destination = (
        Path(shared_filesystem_root)
        / Path(EMG_CONFIG.slurm.samplesheet_editing_path_from_shared_filesystem)
        / Path("for_editing")
        / samplesheet_name
    )
    return destination


def location_where_samplesheet_was_edited(
    cluster_location: Path, shared_filesystem_root: str
) -> Path:
    """
    Determine a path on the shared filesystem (an NFS locaiton available on both datamovers and k8s)
    where a samplesheet can be temporarily saved for editing.
    Parameters
    ----------
    cluster_location: e.g. /nfs/production/path/to/a/run/and/a/samplesheet123.csv
    shared_filesystem_root: e.g. /nfs/public/rw/our/dir or /app/data/

    Returns
    -------
    E.g. /nfs/public/rw/our/dir/temporary_samplesheet_edits/from_editing/samplesheet123.csv
    (if samplesheet_editing_path_from_shared_filesystem config is set to `temporary_samplesheets_edits`)

    """
    samplesheet_name = Path(cluster_location).name
    destination = (
        Path(shared_filesystem_root)
        / Path(EMG_CONFIG.slurm.samplesheet_editing_path_from_shared_filesystem)
        / Path("from_editing")
        / samplesheet_name
    )
    return destination


def move_samplesheet_to_editable_location(
    source: str | Path, timeout=Optional[int]
) -> (FlowRun, Path):
    destination = location_for_samplesheet_to_be_edited(
        source, EMG_CONFIG.slurm.shared_filesystem_root_on_slurm
    )
    logger.info(f"Will move samplesheet to {destination}")
    # copy samplesheet from source to editable location
    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "source": source,
            "target": destination,
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    return flowrun, destination


def move_samplesheet_back_from_editable_location(
    destination: str | Path, timeout=Optional[int]
) -> (FlowRun, Path):
    source = location_where_samplesheet_was_edited(
        destination, EMG_CONFIG.slurm.shared_filesystem_root_on_slurm
    )
    logger.info(f"Will move samplesheet from {source} to {destination}")
    # copy samplesheet from source to editable location
    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "source": source,
            "target": destination,
            "move_command": "cp",
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    return flowrun, destination
