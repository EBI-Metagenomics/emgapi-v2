import logging
import pathlib
import shutil
from collections import defaultdict
from pathlib import Path
from shutil import SameFileError
from typing import Literal

import pandas as pd
from Bio import SeqIO
from mgnify_pipelines_toolkit.constants.tax_ranks import (
    PR2_TAX_RANKS,
    SHORT_PR2_TAX_RANKS,
    SHORT_SILVA_TAX_RANKS,
    SILVA_TAX_RANKS,
)
from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis, Study
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File
from workflows.ena_utils.ena_accession_matching import (
    INSDC_PROJECT_ACCESSION_GLOB,
    INSDC_STUDY_ACCESSION_GLOB,
)
from workflows.ena_utils.sample import ENASampleFields
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
)
from workflows.prefect_utils.flows_utils import (
    django_db_task as task,
)

LOGGER = logging.getLogger(__name__)

DWCREADY_CSV = "_dwcready.csv"

ASV_DATABASES = ("DADA2-SILVA", "DADA2-PR2")
CLOSED_REFERENCE_DATABASES = ("SILVA-SSU", "PR2", "SILVA-LSU", "ITSoneDB", "UNITE")
DATABASES_WITH_TAXIDS = {
    "DADA2-SILVA": "SILVA-SSU.otu",
    "SILVA-SSU": "SILVA-SSU.otu",
    "SILVA-LSU": "SILVA-LSU.otu",
    "ITSoneDB": "ITSone.otu",
}


def _normalise_scalar(value: object) -> object:
    """Return a DwC-R friendly scalar value.

    :param value: Value read from local metadata.
    :return: "NA" when the input is empty, otherwise the original value.
    """
    if value in (None, ""):
        return "NA"
    return value


def get_all_ena_metadata_from_runs(
    mgnify_study_accession: str,
    runs: list[str],
) -> dict[str, pd.DataFrame]:
    """Build run-level ENA metadata tables from local database records.

    Metadata is read through the local Analysis graph, using the linked
    Run, Sample, Study and ena.Sample records. No network calls
    are made.

    :param mgnify_study_accession: MGnify study accession to summarize.
    :param runs: Run accessions from the completed-runs file.
    :return: Mapping of run accession to a one-row metadata dataframe.
    """
    study = Study.public_objects.get(accession=mgnify_study_accession)
    analyses = (
        Analysis.public_objects.filter(
            study=study,
            experiment_type=Analysis.ExperimentTypes.AMPLICON,
            pipeline_version__in=(
                Analysis.PipelineVersions.v6,
                Analysis.PipelineVersions.v6_1,
            ),
            run__isnull=False,
            sample__isnull=False,
        )
        .select_related("run", "sample", "sample__ena_sample", "study", "ena_study")
        .distinct()
    )

    requested_runs = set(runs)
    run_metadata_dict: dict[str, pd.DataFrame] = {}
    for analysis in analyses:
        run = analysis.run
        sample = analysis.sample
        run_accession = run.first_accession
        if not run_accession or run_accession not in requested_runs:
            logging.warning(f"Skipping run {run_accession} not in requested runs")
            continue

        sample_metadata = sample.metadata_preferring_inferred
        if sample.ena_sample:
            sample_metadata = {
                **sample.ena_sample.metadata,
                **sample_metadata,
            }

        metadata = {
            "RunID": run_accession,
            "SampleID": sample.first_accession,
            "StudyID": analysis.study.first_accession,
            "seq_meth": _normalise_scalar(run.instrument_model),
            "decimalLongitude": _normalise_scalar(
                sample_metadata.get(ENASampleFields.LON)
            ),
            "decimalLatitude": _normalise_scalar(
                sample_metadata.get(ENASampleFields.LAT)
            ),
            "depth": _normalise_scalar(sample_metadata.get(ENASampleFields.DEPTH)),
            "temperature": _normalise_scalar(
                sample_metadata.get(ENASampleFields.TEMPERATURE)
            ),
            "salinity": _normalise_scalar(
                sample_metadata.get(ENASampleFields.SALINITY)
            ),
            "collectionDate": _normalise_scalar(
                sample_metadata.get(ENASampleFields.COLLECTION_DATE)
            ),
            "country": _normalise_scalar(sample_metadata.get(ENASampleFields.COUNTRY)),
            "InstitutionCode": _normalise_scalar(
                sample_metadata.get(ENASampleFields.CENTER_NAME)
            ),
        }
        run_metadata_dict[run_accession] = pd.DataFrame(metadata, index=[0])

    return run_metadata_dict


def cleanup_asv_taxa(
    df: pd.DataFrame,
    db: Literal["DADA2-SILVA", "DADA2-PR2"],
) -> pd.DataFrame:
    """Normalize merged ASV records into the DwC-R CSV column layout.

    :param df: Merged ENA metadata and ASV result dataframe.
    :param db: DADA2 reference database label.
    :return: Dataframe with normalized taxonomy values and ordered DwC-R columns.
    """
    cleaned_df = df.rename(
        columns={
            "asv": "ASVID",
            "count": "MeasurementValue",
            "center_name": "InstitutionCode",
        }
    )
    ranks = SILVA_TAX_RANKS if db == "DADA2-SILVA" else PR2_TAX_RANKS
    cleaned_db_string = db.split("-")[1]

    for rank in ranks:
        cleaned_df[rank] = cleaned_df[rank].apply(_split_prefixed_taxon)
        cleaned_df[rank] = cleaned_df[rank].fillna("NA").replace("", "NA")

    cleaned_df["MeasurementUnit"] = "Number of reads"
    cleaned_df["ASVCaller"] = "DADA2"
    cleaned_df["ReferenceDatabase"] = cleaned_db_string
    cleaned_df["TaxAnnotationTool"] = "MAPseq"

    return cleaned_df[
        [
            "ASVID",
            "StudyID",
            "SampleID",
            "RunID",
            "decimalLongitude",
            "decimalLatitude",
            "depth",
            "temperature",
            "salinity",
            "collectionDate",
            "seq_meth",
            "country",
            "InstitutionCode",
            "amplifiedRegion",
            "ASVCaller",
            "ReferenceDatabase",
            "TaxAnnotationTool",
        ]
        + ranks
        + [
            "taxonID",
            "MeasurementUnit",
            "MeasurementValue",
            "dbhit",
            "dbhitIdentity",
            "dbhitStart",
            "dbhitEnd",
            "ASVSeq",
        ]
    ]


def cleanup_closedref_taxa(
    df: pd.DataFrame,
    db: Literal["SILVA-SSU", "PR2", "SILVA-LSU", "ITSoneDB", "UNITE"],
) -> pd.DataFrame:
    """Normalize merged closed-reference records into the DwC-R CSV layout.

    :param df: Merged ENA metadata and closed-reference taxonomy dataframe.
    :param db: Closed-reference database label.
    :return: Dataframe with normalized taxonomy values and ordered DwC-R columns.
    """
    cleaned_df = df.rename(
        columns={
            "count": "MeasurementValue",
            "center_name": "InstitutionCode",
        }
    )
    ranks = PR2_TAX_RANKS if db == "PR2" else SILVA_TAX_RANKS

    for rank in ranks:
        cleaned_df[rank] = cleaned_df[rank].fillna("NA").replace("", "NA")

    cleaned_df["MeasurementUnit"] = "Number of reads"
    cleaned_df["ReferenceDatabase"] = db
    cleaned_df["TaxAnnotationTool"] = "MAPseq"

    return cleaned_df[
        [
            "StudyID",
            "SampleID",
            "RunID",
            "decimalLongitude",
            "decimalLatitude",
            "depth",
            "temperature",
            "salinity",
            "collectionDate",
            "seq_meth",
            "country",
            "InstitutionCode",
            "ReferenceDatabase",
            "TaxAnnotationTool",
        ]
        + ranks
        + [
            "taxonID",
            "MeasurementUnit",
            "MeasurementValue",
        ]
    ]


def _split_prefixed_taxon(value: object) -> object:
    """Strip rank prefixes from taxonomy values.

    :param value: Taxonomy value such as sk__Bacteria.
    :return: Unprefixed taxonomy value, or "NA" when blank.
    """
    if pd.isna(value):
        return "NA"
    text = str(value).strip()
    if not text:
        return "NA"
    if "__" in text:
        _, _, suffix = text.partition("__")
        return suffix or "NA"
    return text


def concatenate_taxa_row(taxa_row: pd.Series, separator: str = ";") -> str:
    """Concatenate taxonomy ranks for matching against OTU taxid files.

    :param taxa_row: Series containing ordered taxonomy rank values.
    :param separator: Separator used by the OTU taxonomy field.
    :return: Concatenated taxonomy string with trailing missing ranks removed.
    """
    concatenated = separator.join(taxa_row.values.astype(str))
    concatenated = concatenated.split(f"{separator}nan")[0]
    concatenated = concatenated.split(f"{separator}NA")[0]
    return concatenated


def generate_taxid_list(taxa_df: pd.DataFrame, db: str, otu_dir: pathlib.Path) -> list:
    """Look up taxonomic identifiers for taxonomy rows.

    Databases without local OTU taxid mappings, or missing OTU files, receive
    "NA" taxids.

    :param taxa_df: Dataframe containing taxonomy rank columns.
    :param db: Reference database label.
    :param otu_dir: Directory containing reference OTU mapping files.
    :return: Taxid values aligned to taxa_df rows.
    """
    otu_filename = DATABASES_WITH_TAXIDS.get(db)
    if not otu_filename:
        return ["NA"] * len(taxa_df)

    otu_file = otu_dir / otu_filename
    if not otu_file.exists():
        LOGGER.warning("Taxid OTU file %s is missing for %s", otu_file, db)
        return ["NA"] * len(taxa_df)

    tax_cols = PR2_TAX_RANKS if db == "PR2" else SILVA_TAX_RANKS
    concatenated_taxa = taxa_df.loc[:, tax_cols].apply(concatenate_taxa_row, axis=1)
    otu_df = pd.read_csv(
        otu_file,
        sep="\t",
        usecols=[1, 2],
        names=["taxa", "taxid"],
        skiprows=1,
    )
    otu_dict = defaultdict(
        lambda: {"taxid": "NA"},
        otu_df.set_index("taxa").to_dict("index"),
    )
    return concatenated_taxa.apply(lambda value: otu_dict[value]["taxid"]).to_list()


def _parse_fasta(fasta_path: Path) -> dict[str, str]:
    """Parse ASV FASTA records into a sequence dictionary.

    :param fasta_path: Path to the ASV FASTA file.
    :return: Mapping of ASV identifier to nucleotide sequence.
    """
    return {record.id: str(record.seq) for record in SeqIO.parse(fasta_path, "fasta")}


def get_asv_dict(
    runs_df: pd.DataFrame,
    root_path: Path,
    db: Literal["DADA2-SILVA", "DADA2-PR2"],
    otu_dir: Path,
) -> dict[str, pd.DataFrame]:
    """Collect ASV taxonomy, count, MAPseq and sequence data by run.

    Runs without complete ASV result files for the requested database are
    skipped.

    :param runs_df: Completed-runs dataframe with run and status columns.
    :param root_path: Directory containing per-run analysis output directories.
    :param db: DADA2 reference database label.
    :param otu_dir: Directory containing reference OTU mapping files.
    :return: Mapping of run accession to ASV result dataframe.
    """
    asv_dict = {}
    for _, run_row in runs_df.iterrows():
        run_acc = run_row["run"]
        analysis_status = run_row["status"]

        if analysis_status != "all_results":
            continue

        mapseq_files = sorted(
            (Path(root_path) / run_acc / "taxonomy-summary" / db).glob(f"*_{db}.mseq")
        )
        if not mapseq_files:
            LOGGER.info(
                "Run %s does not seem to have ASV results for DB %s, with mapseq file missing. Skipping.",
                run_acc,
                db,
            )
            continue
        mapseq_df = pd.read_csv(
            mapseq_files[0],
            sep="\t",
            usecols=[0, 1, 3, 9, 10],
        )
        mapseq_df.columns = ["asv", "dbhit", "dbhitIdentity", "dbhitStart", "dbhitEnd"]

        tax_files = sorted(
            (Path(root_path) / run_acc / "asv").glob(f"*_{db}_asv_tax.tsv")
        )
        if not tax_files:
            LOGGER.info(
                "Run %s does not seem to have ASV results for DB %s, with asv tax file missing. Skipping.",
                run_acc,
                db,
            )
            continue
        run_tax_df = pd.read_csv(tax_files[0], sep="\t")

        count_files = sorted((Path(root_path) / run_acc / "asv").glob("*S-V*/*.tsv"))
        if not count_files:
            LOGGER.info(
                "Run %s missing count files for some reason, cannot generate ASV summaries. Skipping.",
                run_acc,
            )
            continue

        asv_fasta_files = sorted(
            (Path(root_path) / run_acc / "asv").glob("*_asv_seqs.fasta")
        )
        if not asv_fasta_files:
            LOGGER.info(
                "Run %s does not seem to have ASV FASTA sequence file. Skipping.",
                run_acc,
            )
            continue
        asv_fasta_df = pd.DataFrame(
            [
                {"asv": asv, "ASVSeq": seq}
                for asv, seq in _parse_fasta(asv_fasta_files[0]).items()
            ]
        )

        count_dfs = []
        for count_file in count_files:
            amp_region = count_file.stem.split("_")[1]
            count_df = pd.read_csv(count_file, sep="\t")
            count_df["amplifiedRegion"] = amp_region
            count_dfs.append(count_df)

        all_amplified_regions_count_df = pd.concat(count_dfs, ignore_index=True)
        merged_df = all_amplified_regions_count_df.merge(
            run_tax_df,
            left_on="asv",
            right_on="ASV",
        )
        merged_df = merged_df.merge(mapseq_df, on="asv")
        merged_df.pop("ASV")
        merged_df["RunID"] = run_acc
        merged_df = merged_df.merge(asv_fasta_df, on="asv")
        merged_df["taxonID"] = generate_taxid_list(merged_df, db, otu_dir)

        asv_dict[run_acc] = merged_df

    return asv_dict


def get_closedref_dict(
    runs_df: pd.DataFrame,
    root_path: Path,
    db: Literal["SILVA-SSU", "PR2", "SILVA-LSU", "ITSoneDB", "UNITE"],
    otu_dir: Path,
) -> dict[str, pd.DataFrame]:
    """Collect closed-reference taxonomy summaries by run.

    Runs without closed-reference result files for the requested database are
    skipped.

    :param runs_df: Completed-runs dataframe with run and status columns.
    :param root_path: Directory containing per-run analysis output directories.
    :param db: Closed-reference database label.
    :param otu_dir: Directory containing reference OTU mapping files.
    :return: Mapping of run accession to closed-reference result dataframe.
    """
    ranks = PR2_TAX_RANKS if db == "PR2" else SILVA_TAX_RANKS
    short_ranks = SHORT_PR2_TAX_RANKS if db == "PR2" else SHORT_SILVA_TAX_RANKS

    closedref_dict = {}
    for _, run_row in runs_df.iterrows():
        run_acc = run_row["run"]
        status = run_row["status"]

        if status != "all_results":
            continue

        kronatxt_files = sorted(
            (pathlib.Path(root_path) / run_acc / "taxonomy-summary" / f"{db}").glob(
                "*.txt"
            )
        )
        if not kronatxt_files:
            LOGGER.info(
                "Run %s doesn't have closed-reference results for DB %s, skipping it.",
                run_acc,
                db,
            )
            continue

        column_names = ["count"] + ranks
        tax_df = pd.read_csv(kronatxt_files[0], sep="\t", names=column_names)
        tax_df = tax_df.fillna("NA")
        krona_taxranks = [rank + "__" for rank in short_ranks]
        tax_df = tax_df.applymap(
            lambda value: "NA" if value in krona_taxranks else value
        )
        tax_df["RunID"] = run_acc
        tax_df["taxonID"] = generate_taxid_list(tax_df, db, otu_dir)
        closedref_dict[run_acc] = tax_df

    return closedref_dict


def generate_dwcready_summaries(
    mgnify_study_accession: str,
    runs: Path,
    analyses_dir: Path,
    output_prefix: str,
    otu_dir: Path,
) -> None:
    """Generate DwC-R study summary CSV files for one pipeline output directory.

    The function writes one CSV per ASV database/amplified-region combination and
    one CSV per available closed-reference database.

    :param mgnify_study_accession: MGnify study accession to summarize.
    :param runs: CSV file listing run accessions and completion statuses.
    :param analyses_dir: Directory containing per-run analysis output directories.
    :param output_prefix: Prefix for generated *_dwcready.csv files.
    :param otu_dir: Directory containing reference OTU mapping files.
    :raises FileNotFoundError: If analyses_dir does not exist.
    """
    root_path = pathlib.Path(analyses_dir)

    if not root_path.exists():
        raise FileNotFoundError(f"Results path does not exist: {root_path}")

    runs_df = pd.read_csv(runs, names=["run", "status"])
    all_runs = runs_df["run"].to_list()
    run_metadata_dict = get_all_ena_metadata_from_runs(mgnify_study_accession, all_runs)

    for db in ASV_DATABASES:
        asv_dict = get_asv_dict(runs_df, root_path, db, otu_dir)
        all_merged_df = []

        for run in all_runs:
            if run in asv_dict and run in run_metadata_dict:
                run_merged_result = run_metadata_dict[run].merge(
                    asv_dict[run], on="RunID"
                )
                all_merged_df.append(run_merged_result)

        if not all_merged_df:
            LOGGER.info(
                "No ASV results at all for DB %s in entire analysis, won't generate summary for it.",
                db,
            )
            continue

        final_df = pd.concat(all_merged_df, ignore_index=True)
        final_df = cleanup_asv_taxa(final_df, db)

        for amplified_region in final_df["amplifiedRegion"].unique():
            amplified_region_df = final_df.loc[
                final_df["amplifiedRegion"] == amplified_region
            ]
            amplified_region_df.to_csv(
                f"{output_prefix}_{db}_{amplified_region}_dwcready.csv",
                index=False,
                na_rep="NA",
            )

    for db in CLOSED_REFERENCE_DATABASES:
        closedref_dict = get_closedref_dict(runs_df, root_path, db, otu_dir)
        all_merged_df = []

        for run in all_runs:
            if run in closedref_dict and run in run_metadata_dict:
                run_merged_result = run_metadata_dict[run].merge(
                    closedref_dict[run],
                    on="RunID",
                )
                all_merged_df.append(run_merged_result)

        if all_merged_df:
            final_df = pd.concat(all_merged_df, ignore_index=True)
            final_df = cleanup_closedref_taxa(final_df, db)
            final_df.to_csv(
                f"{output_prefix}_closedref_{db}_dwcready.csv",
                index=False,
                na_rep="NA",
            )
        else:
            LOGGER.info(
                "No results at all for DB %s in entire analysis, won't generate summary for it.",
                db,
            )


def organise_dwcr_summaries(all_study_summaries) -> defaultdict[list]:
    """Group DwC-R summary files by analysis/database label.

    :param all_study_summaries: Iterable of *_dwcready.csv paths.
    :return: Mapping of summary label to paths with that label.
    """
    summaries_dict = defaultdict(list)

    for summary_path in all_study_summaries:
        temp_lst = summary_path.stem.split("_")
        summary_db_label = "_".join(temp_lst[1:3])
        summaries_dict[summary_db_label].append(summary_path)

    return summaries_dict


def merge_dwcr_summaries(analyses_dir: str | Path, output_prefix: str) -> None:
    """Merge partial DwC-R summary files in a directory.

    Files are grouped by the label embedded in their filename. Groups with a
    single file are copied to the merged output path.

    :param analyses_dir: Directory containing partial *_dwcready.csv files.
    :param output_prefix: Prefix for merged output CSV files.
    """
    all_dwcr_summaries = Path(analyses_dir).glob("*_dwcready.csv")
    summaries_dict = organise_dwcr_summaries(all_dwcr_summaries)

    for db_label, summaries in summaries_dict.items():
        merged_summary_name = f"{output_prefix}_{db_label}_dwcready.csv"
        if len(summaries) > 1:
            res_df = pd.read_csv(summaries[0])
            for summary in summaries[1:]:
                curr_df = pd.read_csv(summary)
                res_df = pd.concat([res_df, curr_df], ignore_index=True)

            res_df.to_csv(merged_summary_name, index=False, na_rep="NA")
        elif len(summaries) == 1:
            LOGGER.info(
                "Only one summary (%s) so will use that as %s",
                summaries[0],
                merged_summary_name,
            )
            try:
                shutil.copyfile(summaries[0], merged_summary_name)
            except SameFileError:
                pass


@flow()
def generate_dwc_ready_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path,
    refdb_otus_dir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> list[Path] | None:
    """Generate DwC-R summaries for one amplicon pipeline run directory.

    :param mgnify_study_accession: MGnify study accession to summarize.
    :param pipeline_outdir: Directory containing per-run pipeline outputs.
    :param refdb_otus_dir: Directory containing reference OTU mapping files.
    :param completed_runs_filename: Name of the completed-runs CSV in
        pipeline_outdir.
    :return: Generated DwC-R summary CSV paths, or None when Prefect returns
        no task result.
    """
    logger = get_run_logger()

    study = Study.objects.get(accession=mgnify_study_accession)

    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline
    pipeline_run_dir = Directory(
        path=pipeline_outdir,
        rules=[DirectoryExistsRule],
    )
    summary_dir = Directory(
        path=(
            study.results_dir_path
            / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
            / "summaries"
        ),
    )
    summary_dir.path.mkdir(parents=True, exist_ok=True)

    logger.info(pipeline_outdir)
    logger.info(completed_runs_filename)
    logger.info(
        f"Generating Darwin Core Ready (DwC-R) summary files for a pipeline execution of study {mgnify_study_accession}"
    )
    pipeline_run_dir.files.append(
        File(
            path=pipeline_run_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    runs = pipeline_run_dir.files[0].path
    logger.info(f"Expecting to find taxonomy summaries in {pipeline_run_dir.path}")
    logger.info(f"Using runs from {runs}")

    prefix = f"{summary_dir.path}/{pipeline_run_dir.path.name}"

    logger.debug(
        f"For DwC-R summary, {pipeline_run_dir.path = }, {runs = }, {prefix = }"
    )
    logger.debug(f"Glob of input_path is {list(pipeline_run_dir.path.glob('*'))}")

    content = runs.read_text()
    logger.debug(f"Content of runs file is\n{content}")

    generate_dwcready_summaries(
        mgnify_study_accession=mgnify_study_accession,
        runs=runs,
        analyses_dir=pipeline_run_dir.path,
        otu_dir=Path(refdb_otus_dir),
        output_prefix=prefix,
    )

    generated_files = list(summary_dir.path.glob("*_dwcready.csv"))

    logger.info(generated_files)

    return generated_files


@task()
def merge_dwc_ready_summaries(
    mgnify_study_accession: str,
    cleanup_partials: bool = False,
    bludgeon: bool = True,
) -> list[Path] | None:
    """Merge partial DwC-R summaries for a study into study-level files.

    :param mgnify_study_accession: MGnify study accession whose summaries should
        be merged.
    :param cleanup_partials: Remove partial summary files after successful merge.
    :param bludgeon: Delete existing merged summaries before writing replacements.
    :return: Merged DwC-R summary CSV paths, or an empty list when nothing is
        merged.
    """
    logger = get_run_logger()

    study = Study.objects.get(accession=mgnify_study_accession)

    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline
    study_dir = Directory(
        path=(
            study.results_dir_path
            / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
        ),
        rules=[DirectoryExistsRule],
    )
    summary_dir = Directory(
        path=study_dir.path / "summaries",
        rules=[DirectoryExistsRule],
    )

    logger.info(f"Merging DwC-R summaries for {study}, in {summary_dir}")

    if not summary_dir:
        logger.warning(
            f"Study {study} has no results_dir, so cannot merge DwC-R summaries"
        )
        return []

    logger.debug(f"Glob of dir is {list(summary_dir.path.glob('*'))}")
    existing_merged_files = list(
        study_dir.path.glob(f"{INSDC_PROJECT_ACCESSION_GLOB}{DWCREADY_CSV}")
    ) + list(study_dir.path.glob(f"{INSDC_STUDY_ACCESSION_GLOB}{DWCREADY_CSV}"))
    if existing_merged_files:
        logger.warning(
            f"{len(existing_merged_files)} DwC-R summaries already exist in {study_dir.path}"
        )
    if bludgeon:
        for existing_merged_file in existing_merged_files:
            logger.warning(f"Deleting {existing_merged_file}")
            existing_merged_file.unlink()

    summary_files = list(summary_dir.path.glob(f"*{DWCREADY_CSV}"))
    logger.info(
        f"There appear to be {len(summary_files)} DwC-R summary files in {summary_dir.path}"
    )

    logger.info(
        f"Study results_dir, where DwC-R summaries will be merged, is {study_dir.path}"
    )

    merge_dwcr_summaries(
        analyses_dir=summary_dir.path,
        output_prefix=f"{study_dir.path}/{study.first_accession}",
    )

    generated_files = list(
        study_dir.path.glob(f"{study.first_accession}*{DWCREADY_CSV}")
    )

    if not generated_files:
        logger.warning(f"No DwC-R summary was merged in {study_dir.path}")
        return []

    logger.info(f"These are the merged DwC-R summary files: {generated_files}")

    if cleanup_partials:
        for file in summary_files:
            logger.info(f"Removing partial study summary file {file}")
            assert not file.name.startswith(
                study.first_accession
            )  # ensure we do not delete merged files
            file.unlink()


@task()
def add_dwcr_summaries_to_downloads(mgnify_study_accession: str):
    """Register generated DwC-R summary files as study downloads.

    :param mgnify_study_accession: MGnify study accession whose generated DwC-R
        summaries should be added to download metadata.
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)
    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline

    summary_dir = (
        study.results_dir_path
        / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
    )

    # Add ASV DwC-R summary files

    dada2_study_summary_files = list(
        Path(summary_dir).glob(f"{study.first_accession}_DADA2*{DWCREADY_CSV}")
    )

    for summary_file in dada2_study_summary_files:
        db = summary_file.stem.split("_")[1].lstrip("DADA2_")
        region = summary_file.stem.split("_")[2]
        try:
            study.add_download(
                DownloadFile(
                    path=Path("study-summaries") / summary_file.name,
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=f"study_summary.{pipeline_config.pipeline_version}.{pipeline_config.pipeline_name}",
                    file_type=DownloadFileType.CSV,
                    short_description=f"DwC-Ready summary of {region} ASV taxonomies using {db} as ref DB",
                    long_description=f"DwC-Ready summary of {region} ASV taxonomies using {db} as ref DB, across all runs in the study",
                    alias=summary_file.name,
                )
            )
        except FileExistsError:
            logger.warning(
                f"File {summary_file} already exists in downloads list, skipping"
            )
        logger.info(f"Added {summary_file} to downloads of {study}")

    # Add closed reference DwC-R summary files
    closedref_study_summary_files = list(
        Path(summary_dir).glob(f"{study.first_accession}_closedref*{DWCREADY_CSV}")
    )
    for summary_file in closedref_study_summary_files:
        db = summary_file.stem.split("_")[2]
        try:
            study.add_download(
                DownloadFile(
                    path=Path("study-summaries") / summary_file.name,
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=f"study_summary.{pipeline_config.pipeline_version}.{pipeline_config.pipeline_name}",
                    file_type=DownloadFileType.CSV,
                    short_description=f"DwC-Ready summary of closed-ref taxonomies using {db} as ref DB",
                    long_description=f"DwC-Ready summary of closed-reference taxonomies using {db} as ref DB, across all runs in the study",
                    alias=summary_file.name,
                )
            )
        except FileExistsError:
            logger.warning(
                f"File {summary_file} already exists in downloads list, skipping"
            )
        logger.info(f"Added {summary_file} to downloads of {study}")

    study.refresh_from_db()
    logger.info(
        f"Study download aliases are now {[d.alias for d in study.downloads_as_objects]}"
    )
