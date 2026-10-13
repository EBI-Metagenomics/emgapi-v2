"""Protein ingestion: gives MGYPs to the proteins of annotated assembly analyses, and stages their rows for the dataset update."""

import logging
import re
from itertools import groupby
from typing import NamedTuple

from analyses.models import Analysis
from proteins.accession.accession import accession
from proteins.accession.inputs import read_input
from proteins.models import AssemblyProteins, GeneCaller
from proteins.routers import mgyp_connection

logger = logging.getLogger(__name__)

MGYA = re.compile(r"MGYA\d+")
ERZ = re.compile(r"ERZ\d+")


class IngestionResult(NamedTuple):
    analysis_accession: str
    skipped: bool  # the assembly proteins record was already in the MGYP database
    proteins: int  # distinct proteins of the analysis


def annotated_assembly_analyses():
    """Assembly analyses whose annotations emgapi-v2 has imported, of any pipeline version.

    Leaves out the analyses that took their proteins from a gene calling (workflows.AssemblyGeneCalling):
    the gene_call_assembly flow has already given MGYPs to those proteins.
    """
    return (
        Analysis.objects.filter_by_statuses(
            [Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
        )
        .filter(assembly__isnull=False, gene_calling__isnull=True)
        .select_related("assembly", "study")
    )


def find_pending_analyses(study_accession: str | None = None) -> list[Analysis]:
    """Assembly analyses with annotations imported whose assembly proteins record is not in the MGYP database.

    Of several analyses of one assembly with one pipeline version, the first is taken.

    :param study_accession: An MGYS accession, to list only its analyses.
    :return: The pending analyses, ordered by study and id.
    """
    analyses = annotated_assembly_analyses().order_by("study_id", "id")
    if study_accession:
        analyses = analyses.filter(study__accession=study_accession)
    # Loads every assembly proteins key into memory. Good enough for the prototype.
    known = set(
        AssemblyProteins.objects.filter(
            source=AssemblyProteins.Source.ASSEMBLY_ANALYSIS
        ).values_list("assembly_accession", "pipeline_version")
    )
    pending = []
    for analysis in analyses:
        key = (analysis.assembly.first_accession, analysis.pipeline_major_minor)
        if key not in known:
            known.add(key)
            pending.append(analysis)
    return pending


def chunk_by_study(analyses: list[Analysis], size: int) -> list[list[str]]:
    """The MGYAs in chunks of up to `size`. A chunk has analyses of one study only.

    :param analyses: Analyses with their study, ordered by study.
    :param size: The largest chunk.
    :return: Lists of MGYA accessions.
    """
    chunks = []
    for _, of_study in groupby(analyses, key=lambda analysis: analysis.study_id):
        accessions = [analysis.accession for analysis in of_study]
        chunks += [
            accessions[start : start + size]
            for start in range(0, len(accessions), size)
        ]
    return chunks


def resolve_accession(accession: str) -> list[str]:
    """The MGYAs to ingest for one MGYA, or for one ERZ (each annotated analysis of that assembly).

    :param accession: An MGYA or an ERZ accession.
    :return: MGYA accessions.
    :raises ValueError: When the accession is neither, or matches no annotated analysis.
    """
    analyses = annotated_assembly_analyses().order_by("id")
    if MGYA.fullmatch(accession):
        analyses = analyses.filter(accession=accession)
    elif ERZ.fullmatch(accession):
        analyses = analyses.filter(assembly__ena_accessions__contains=[accession])
    else:
        raise ValueError(f"{accession} is neither an MGYA nor an ERZ accession")
    accessions = list(analyses.values_list("accession", flat=True))
    if not accessions:
        raise ValueError(f"{accession} matches no assembly analysis with annotations")
    return accessions


def ingest_analysis(analysis_accession: str) -> IngestionResult:
    """Gives MGYPs to the proteins of one analysis, and stages its rows, unless its assembly proteins record exists.

    :param analysis_accession: An MGYA.
    :return: What happened.
    """
    analysis = Analysis.objects.select_related("assembly").get(
        accession=analysis_accession
    )
    erz = analysis.assembly.first_accession
    version = analysis.pipeline_major_minor
    if AssemblyProteins.objects.filter(
        assembly_accession=erz,
        source=AssemblyProteins.Source.ASSEMBLY_ANALYSIS,
        pipeline_version=version,
    ).exists():
        logger.info(
            f"The assembly proteins record {erz} {version} exists; skipping {analysis_accession}"
        )
        return IngestionResult(analysis_accession, skipped=True, proteins=0)

    files = analysis.predicted_genes_files()
    input = read_input(
        files.proteins,
        files.genes,
        files.contigs,
        files.contig_map,
        gene_callers=GeneCaller.of_pipeline_version(version),
    )
    ids = accession(
        mgyp_connection,
        erz,
        AssemblyProteins.Source.ASSEMBLY_ANALYSIS.value,
        version,
        input,
        analysis_accession=analysis_accession,
    )
    logger.info(
        f"Ingested {analysis_accession} ({erz} {version}): {len(ids)} proteins,"
        f" {len(input.occurrences)} genes, {len(input.contigs)} contigs"
    )
    return IngestionResult(analysis_accession, skipped=False, proteins=len(ids))
