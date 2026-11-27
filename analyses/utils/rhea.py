"""Import unique ASAv6 reaction participants per analysis."""

from pathlib import Path

from django.db import transaction

from analyses.models import Analysis, AnalysisMoleculeIndex
from analyses.utils.export_rhea_index import read_chebi_ids


def import_rhea(analysis_id: int, path: Path) -> int:
    """Replace the analysis molecule set after validating its Rhea table.

    :param analysis_id: The analysis database identifier.
    :param path: The plain or compressed ASAv6 proteins2rhea table.
    :return: The number of unique ChEBI identifiers.
    :raises ValueError: If a row does not match the pipeline format.
    """
    compounds = read_chebi_ids(path)
    with transaction.atomic():
        Analysis.objects.select_for_update().get(pk=analysis_id)
        AnalysisMoleculeIndex.objects.update_or_create(
            analysis_id=analysis_id, defaults={"chebi_ids": compounds}
        )
    return len(compounds)
