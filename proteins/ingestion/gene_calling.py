"""Protein accessioning of a gene calling: gives MGYPs to the proteins of the gene calling pipeline, stages their rows, and writes the FAA with MGYPs for the assembly analysis."""

import logging
from pathlib import Path
from typing import NamedTuple

from analyses.models import PredictedGenesFiles
from proteins.accession.accession import accession, write_proteins_with_mgyps
from proteins.accession.inputs import read_input
from proteins.models import AssemblyProteins, GeneCaller
from proteins.routers import mgyp_connection

logger = logging.getLogger(__name__)


class GeneCallingResult(NamedTuple):
    assembly_proteins_id: int  # the record in the MGYP database
    proteins: int  # distinct proteins of the gene calling
    proteins_with_mgyps: Path  # the FAA for the assembly analysis


def accession_gene_calling(
    assembly_accession: str,
    pipeline_version: str,
    analysis_accession: str,
    files: PredictedGenesFiles,
    proteins_with_mgyps: Path,
) -> GeneCallingResult:
    """Gives MGYPs to the proteins of one gene calling, stages its rows, and writes the FAA with MGYPs.

    Unlike the ingestion of an analysis, it does not skip an existing record: a second run returns
    the same MGYPs, and writes the same file again. So a crash between the commit and the file is safe.

    :param assembly_accession: The ERZ.
    :param pipeline_version: The gene calling pipeline's major.minor.
    :param analysis_accession: The MGYA of the assembly analysis that takes the proteins.
    :param files: The outputs of the gene calling pipeline, and the contigs it called genes on.
    :param proteins_with_mgyps: Where to write the FAA with MGYPs.
    :return: What happened.
    """
    source = AssemblyProteins.Source.GENE_CALLING
    input = read_input(
        files.proteins,
        files.genes,
        files.contigs,
        files.contig_map,
        gene_callers=GeneCaller.of_pipeline_version(pipeline_version, source),
    )
    ids = accession(
        mgyp_connection,
        assembly_accession,
        source.value,
        pipeline_version,
        input,
        analysis_accession,
    )
    record = AssemblyProteins.objects.get(
        assembly_accession=assembly_accession,
        source=source,
        pipeline_version=pipeline_version,
    )
    write_proteins_with_mgyps(proteins_with_mgyps, input.occurrences, ids)
    logger.info(
        f"Accessioned the gene calling {assembly_accession} {pipeline_version} for {analysis_accession}:"
        f" {len(ids)} proteins, {len(input.occurrences)} genes, {len(input.contigs)} contigs."
        f" Wrote {proteins_with_mgyps}"
    )
    return GeneCallingResult(record.id, len(ids), proteins_with_mgyps)
