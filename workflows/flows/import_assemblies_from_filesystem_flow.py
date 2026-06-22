from pathlib import Path

import pandas as pd
from django.db import transaction
from pandera.typing import DataFrame
from prefect import get_run_logger
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.data_io_utils.miassembler_utils import (
    miassembler_run_output_dir,
)
from workflows.ena_utils.abstract import ENAPortalResultType
from workflows.ena_utils.analysis import ENAAnalysisFields, ENAAnalysisQuery
from workflows.ena_utils.ena_api_requests import (
    get_study_readruns_from_ena,
)
from workflows.ena_utils.ena_auth import dcc_auth
from workflows.ena_utils.requestors import ENAAPIRequest
from workflows.flows.assemble_study_tasks.miassembler_reports import (
    AssembledRunsReport,
    QcFailedRunsReport,
    load_assembled_runs_report,
    load_coverage_report,
    load_qc_failed_runs_report,
)
from workflows.flows.shared.study_tasks import get_or_create_mgnify_study
from workflows.prefect_utils.analyses_models_helpers import (
    mark_assembly_status,
)
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
)
from workflows.prefect_utils.flows_utils import (
    django_db_task as task,
)


@task(name="Validate assembly output directory")
def validate_assembly_output_dir(nextflow_outdir: str | Path) -> Path:
    """
    Validate that the miassembler output directory exists and has its required report.
    """
    logger = get_run_logger()
    output_dir = Path(nextflow_outdir)

    if not output_dir.exists():
        raise FileNotFoundError(f"Assembly output directory not found: {output_dir}")
    if not output_dir.is_dir():
        raise ValueError(f"Assembly output path is not a directory: {output_dir}")

    assembled_runs_csv = output_dir / "assembled_runs.csv"
    if not assembled_runs_csv.is_file():
        raise FileNotFoundError(
            f"Expected miassembler assembled-runs report at {assembled_runs_csv}"
        )

    # TODO: add some more validation here

    logger.info(f"Assembly output directory validated: {output_dir}")
    return output_dir


@task(name="Load assembled runs report")
def load_assembled_runs(nextflow_outdir: str | Path) -> DataFrame[AssembledRunsReport]:
    """
    Read and validate the miassembler completed-runs report from an output directory.
    """
    logger = get_run_logger()
    assembled_runs_csv = Path(nextflow_outdir) / "assembled_runs.csv"
    report = load_assembled_runs_report(assembled_runs_csv)
    logger.info(f"Read {len(report)} assembled run records")
    return report


@task(name="Load QC failed runs report")
def load_qc_failed_runs(nextflow_outdir: str | Path) -> DataFrame[QcFailedRunsReport]:
    """
    Read and validate the optional miassembler QC-failed runs report.
    """
    logger = get_run_logger()
    qc_failed_csv = Path(nextflow_outdir) / "qc_failed_runs.csv"

    if not qc_failed_csv.is_file():
        logger.info(f"No QC failed runs report found at {qc_failed_csv}")
        return pd.DataFrame()

    report = load_qc_failed_runs_report(qc_failed_csv)
    logger.info(f"Read {len(report)} QC failed run records")
    return report


@task(name="Validate completed assembly files")
def validate_completed_assembly_files(
    nextflow_outdir: str | Path,
    study_accession: str,
    assembled_runs_report: DataFrame[AssembledRunsReport],
) -> None:
    """
    Validate that every completed miassembler row has its expected output files.
    """
    logger = get_run_logger()
    output_dir = Path(nextflow_outdir)

    for record in assembled_runs_report.to_dict("records"):
        run_accession = record["run_accession"]
        assembly_dir = (
            miassembler_run_output_dir(output_dir, study_accession, run_accession)
            / "assembly"
            / record["assembler_name"].lower()
            / record["assembler_version"]
        )
        contigs_path = assembly_dir / f"{run_accession}_cleaned.contigs.fa.gz"
        if not contigs_path.is_file():
            raise FileNotFoundError(
                f"Expected miassembler contigs for run {run_accession} at "
                f"{contigs_path}"
            )

        coverage_report_path = (
            assembly_dir / "coverage" / f"{run_accession}_coverage.json"
        )
        if not coverage_report_path.is_file():
            raise FileNotFoundError(
                f"Expected miassembler coverage report for run {run_accession} at "
                f"{coverage_report_path}"
            )

    logger.info(
        f"Validated miassembler output files for {len(assembled_runs_report)} "
        "completed assembly records"
    )


@task(name="Import completed assembly")
def import_completed_assembly(
    reads_mgnify_study_id: int,
    tpa_mgnify_study_accession: str | None,
    run_id: int,
    nextflow_outdir: Path,
    record: dict[str, str],
    study_accession: str,
    ena_assembly_record: dict[str, str],
    overwrite_existing_assemblies: bool = False,
) -> int:
    """
    Create or update an Assembly from one completed miassembler report row.

    :param reads_mgnify_study_id: MGnify Study ID for the reads used to produce
        the assembly.
    :param tpa_mgnify_study_accession: MGnify Study accession for the separate
        assembly submission study when importing TPA assemblies. Pass ``None`` for
        non-TPA imports, where the assembly belongs to the reads study.
    :param run_id: MGnify Run ID for the assembled read run.
    :param nextflow_outdir: miassembler/Nextflow output directory containing the
        run-level assembly files.
    :param record: Completed row from ``assembled_runs.csv``.
    :param study_accession: ENA accession used in the miassembler output path.
    :param ena_assembly_record: ENA analysis record for the uploaded assembly.
    :param overwrite_existing_assemblies: Allow updating an existing assembly row for
        the same run/sample pair instead of failing.
    :return: The created or updated Assembly ID.
    """
    logger = get_run_logger()
    reads_mgnify_study = analyses.models.Study.objects.get(id=reads_mgnify_study_id)
    assembly_submission_mgnify_study = (
        analyses.models.Study.objects.get(accession=tpa_mgnify_study_accession)
        if tpa_mgnify_study_accession is not None
        else None
    )
    ena_study = (
        assembly_submission_mgnify_study.ena_study
        if assembly_submission_mgnify_study
        else reads_mgnify_study.ena_study
    )
    # TODO: remove the ena_study branch once ENA study handling no longer depends on
    # legacy assembly-study objects.

    run_accession = record["run_accession"]
    run = analyses.models.Run.objects.get(id=run_id)

    with transaction.atomic():
        assembler = analyses.models.Assembler.objects.get(
            name=record["assembler_name"],
            version=record["assembler_version"],
        )
        # Create the assembly row for this run.
        assembly, created = (
            analyses.models.Assembly.objects.get_or_create_for_run_and_sample(
                run=run,
                sample=run.sample,
                reads_study=reads_mgnify_study,
                ena_study=ena_study,
                defaults={
                    "is_private": run.is_private,
                    "webin_submitter": run.webin_submitter,
                },
            )
        )
        # This bit here to is to prevent overwriting existing assemblies by default.
        if not created and not overwrite_existing_assemblies:
            raise ValueError(
                f"Assembly for run {run_accession} already exists in the database"
            )

        assembly.assembler = assembler
        assembly.reads_study = reads_mgnify_study
        assembly.assembly_study = assembly_submission_mgnify_study
        # TPA imports follow the submission study.
        assembly.ena_study = ena_study
        assembly.is_private = run.is_private
        assembly.webin_submitter = run.webin_submitter
        assembly.dir = str(
            miassembler_run_output_dir(
                nextflow_outdir,
                study_accession,
                run_accession,
            )
        )
        assembly.save()

        fields = ENAAnalysisFields
        if ena_assembly_accession := ena_assembly_record.get(fields.ANALYSIS_ACCESSION):
            assembly.add_erz_accession(ena_assembly_accession)
        if generated_ftp := ena_assembly_record.get(fields.GENERATED_FTP):
            assembly.metadata[fields.GENERATED_FTP] = generated_ftp
            assembly.save()

        coverage_report_path = (
            assembly.dir_with_miassembler_suffix
            / "coverage"
            / f"{assembly.runs_label}_coverage.json"
        )
        coverage_report = load_coverage_report(coverage_report_path)
        assembly.update_coverage_metadata_from_report(coverage_report)

        mark_assembly_status(
            assembly,
            status=assembly.AssemblyStates.ASSEMBLY_UPLOADED,
            unset_statuses=[
                assembly.AssemblyStates.ASSEMBLY_FAILED,
                assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
                assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED,
                assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
            ],
        )
        logger.info(
            f"{'Created' if created else 'Updated'} assembly {assembly.id} for run {run_accession}"
        )
        return assembly.id


@task(name="Get study assembly records from ENA")
def get_study_assembly_records_from_ena(
    study_accession: str,
    mgnify_study_accession: str,
    limit: int,
) -> list[dict[str, str]]:
    """
    Fetch ENA assembly records with run accessions for pre-import validation.
    """
    logger = get_run_logger()
    mgnify_study = analyses.models.Study.objects.get(accession=mgnify_study_accession)
    fields = ENAAnalysisFields
    ena_auth = dcc_auth if mgnify_study.is_private else None

    records = ENAAPIRequest(
        result=ENAPortalResultType.ANALYSIS,
        fields=[
            fields.RUN_ACCESSION,
            fields.ANALYSIS_ALIAS,
            fields.ANALYSIS_ACCESSION,
            fields.GENERATED_FTP,
        ],
        limit=limit,
        query=ENAAnalysisQuery(study_accession=study_accession)
        | ENAAnalysisQuery(secondary_study_accession=study_accession),
    ).get(auth=ena_auth, raise_on_empty=False)

    logger.info(f"Fetched {len(records)} assembly records with run accessions from ENA")
    return records


@task(name="Validate assembly exists in ENA")
def validate_assembly_exists_in_ena(
    run_accession: str,
    ena_assembly_records: list[dict[str, str]],
) -> dict[str, str]:
    """
    Validate that ENA assembly refresh found an assembly for a completed run.
    """
    fields = ENAAnalysisFields

    for record in ena_assembly_records:
        run_accessions = record.get(fields.RUN_ACCESSION, "")
        analysis_alias = record.get(fields.ANALYSIS_ALIAS, "")
        record_run_accessions = {
            accession.strip()
            for accession in f"{run_accessions};{analysis_alias}".replace(
                ",", ";"
            ).split(";")
            if accession.strip()
        }
        if run_accession in record_run_accessions:
            return record

    raise ValueError(
        f"Assembly for run {run_accession} was not found in ENA. "
        "Assemblies must be uploaded to ENA before importing "
        "miassembler filesystem outputs."
    )


@task(name="Create assembly import report artifact")
def create_import_report_artifact(
    study_accession: str,
    nextflow_outdir: Path | str,
    imported_assemblies: list[dict[str, str | int]],
    qc_failed_runs: list[dict[str, str]],
) -> None:
    """
    Publish a table artifact summarizing imported and QC-failed miassembler runs.
    """
    table = []

    for imported_assembly in imported_assemblies:
        assembly_id = imported_assembly["assembly_id"]
        table.append(
            {
                "run_accession": imported_assembly["run_accession"],
                "result": "imported",
                "assembly_id": assembly_id,
                "assembler_name": imported_assembly["assembler_name"],
                "assembler_version": imported_assembly["assembler_version"],
            }
        )

    for qc_failed_run in qc_failed_runs:
        table.append(
            {
                "run_accession": qc_failed_run["run_accession"],
                "result": "qc_failed_not_imported",
                "assembly_id": "",
                "assembler_name": "",
                "assembler_version": "",
                "reason": qc_failed_run["reason"],
            }
        )

    create_table_artifact(
        key="miassembler-filesystem-import-report",
        table=table,
        description=(
            f"miassembler filesystem import report for {study_accession} from "
            f"`{nextflow_outdir}`. Imported {len(imported_assemblies)} assemblies; "
            f"reported {len(qc_failed_runs)} QC-failed runs without importing them."
        ),
    )


@flow(
    flow_run_name="Import miassembler - assemblies from {nextflow_outdir}",
)
def import_assemblies_from_filesystem_flow(
    study_accession: str,
    nextflow_outdir: str | Path,
    fetch_read_runs_from_ena: bool = True,
    assembled_study_accession: str | None = None,
    overwrite_existing_assemblies: bool = False,
) -> dict[str, list[int] | list[dict[str, str]]]:
    """
    Import miassembler outputs into Assembly records.

    This flow doesn't support co-assemblies at the moment.

    The input directory must be a miassembler/Nextflow output directory containing
    `assembled_runs.csv`. Completed assemblies are imported from that report, using
    the standard per-study/per-run output directory layout.

    This flow assumes the assemblies were already uploaded to ENA. ENA Portal is
    refreshed before import, and each completed run must resolve to an ENA assembly
    accession returned by that refresh.

    TPA imports provide a separate `assembled_study_accession`; in that case the
    imported assemblies are linked to that study through `assembly_study`.

    :param study_accession: ENA accession of the reads study to import from.
    :param assembled_study_accession: ENA accession of the study that receives the
        assemblies. For non-TPA imports, leave this one empty.
    :param nextflow_outdir: miassembler/Nextflow output directory to import from.
    :param fetch_read_runs_from_ena: Whether to refresh read runs from ENA before import.
    :param overwrite_existing_assemblies: Allow importing over existing assembly rows
        for the same run/sample pair instead of failing.
    """
    logger = get_run_logger()

    validated_outdir: Path = validate_assembly_output_dir(nextflow_outdir)

    assembled_runs_report = load_assembled_runs(validated_outdir)
    if assembled_runs_report.empty:
        raise ValueError(
            "There are no assemblies to import, the assembled_runs.csv is empty."
        )

    qc_failed_runs_report = load_qc_failed_runs(validated_outdir)

    validate_completed_assembly_files(
        validated_outdir,
        study_accession,
        assembled_runs_report,
    )

    reads_mgnify_study_id = get_or_create_mgnify_study(study_accession)
    reads_mgnify_study = analyses.models.Study.objects.get(id=reads_mgnify_study_id)

    assembly_submission_mgnify_study = None
    if assembled_study_accession is not None:
        assembly_submission_mgnify_id = get_or_create_mgnify_study(
            assembled_study_accession
        )
        assembly_submission_mgnify_study = analyses.models.Study.objects.get(
            id=assembly_submission_mgnify_id
        )

    is_tpa = (
        assembly_submission_mgnify_study is not None
        and assembly_submission_mgnify_study
        != reads_mgnify_study  # in case someone provides the same accession here
    )

    if fetch_read_runs_from_ena:
        read_runs = get_study_readruns_from_ena(
            reads_mgnify_study.first_accession,
            limit=EMG_CONFIG.ena.portal_max_readruns_to_fetch,
            raise_on_empty=False,
        )
        logger.info(f"Fetched or refreshed {len(read_runs)} read runs from ENA")

    study_containing_assemblies = (
        assembly_submission_mgnify_study if is_tpa else reads_mgnify_study
    )

    ena_assembly_records = get_study_assembly_records_from_ena(
        study_containing_assemblies.ena_study.accession,
        study_containing_assemblies.accession,
        len(assembled_runs_report),
    )

    imported_assembly_ids: list[int] = []
    imported_assemblies: list[dict[str, str | int]] = []
    for record in assembled_runs_report.to_dict("records"):

        logger.info(f"Importing assembly for run {record['run_accession']}")

        run = analyses.models.Run.objects.get(
            ena_accessions__contains=[record["run_accession"]]
        )
        ena_assembly_record = validate_assembly_exists_in_ena(
            record["run_accession"],
            ena_assembly_records,
        )
        assembly_id = import_completed_assembly(
            reads_mgnify_study_id,
            assembly_submission_mgnify_study.accession if is_tpa else None,
            run.id,
            validated_outdir,
            record,
            study_accession,
            ena_assembly_record,
            overwrite_existing_assemblies=overwrite_existing_assemblies,
        )
        imported_assembly_ids.append(assembly_id)
        imported_assemblies.append(
            {
                "run_accession": record["run_accession"],
                "assembler_name": record["assembler_name"],
                "assembler_version": record["assembler_version"],
                "assembly_id": assembly_id,
            }
        )

    qc_failed_runs: list[dict[str, str]] = qc_failed_runs_report.to_dict("records")

    create_import_report_artifact(
        study_accession,
        validated_outdir,
        imported_assemblies,
        qc_failed_runs,
    )

    summary = {
        "imported": imported_assembly_ids,
        "qc_failed": qc_failed_runs,
    }
    logger.info(f"Assembly filesystem import summary: {summary}")
    return summary
