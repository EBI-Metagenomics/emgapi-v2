from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from Bio import SeqIO
from pydantic import BaseModel, ConfigDict, Field

from activate_django_first import EMG_CONFIG

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis, Study
from workflows.ena_utils.sample import ENASampleFields
from workflows.flows.analysis.summaries.amplicon.shared.parquet.utils import (
    write_amplicon_taxonomy_summary_parquet,
)
from workflows.flows.analysis.summaries.amplicon.shared.schema import (
    SUMMARY_SCHEMA_VERSION,
    TAXONOMY_COLUMNS,
    coerce_taxon_id,
    deepest_taxon_name,
    ensure_canonical_columns,
    make_event_id,
    make_uuid,
    normalise_empty,
    strip_taxon_prefix,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task

PARTIALS_DIRNAME = "partials"
TAXONOMY_PARQUET_SUFFIX = "_taxonomy.parquet"
logger = logging.getLogger(__name__)

SILVA_TAX_RANKS = [
    "Superkingdom",
    "Kingdom",
    "Phylum",
    "Class",
    "Order",
    "Family",
    "Genus",
    "Species",
]
PR2_TAX_RANKS = [
    "Domain",
    "Supergroup",
    "Division",
    "Subdivision",
    "Class",
    "Order",
    "Family",
    "Genus",
    "Species",
]

ASV_DATABASES_AND_TAX_RANKS = (
    ("DADA2-SILVA", SILVA_TAX_RANKS),
    ("DADA2-PR2", PR2_TAX_RANKS),
)
CLOSED_REFERENCE_DATABASES = ("SILVA-SSU", "PR2", "SILVA-LSU", "ITSoneDB", "UNITE")

RANK_COLUMN_MAP = {
    "Superkingdom": "superkingdom",
    "Domain": "domain",
    "Kingdom": "kingdom",
    "Supergroup": "supergroup",
    "Phylum": "phylum",
    "Division": "division",
    "Subdivision": "subdivision",
    "Class": "class",
    "Order": "order",
    "Family": "family",
    "Genus": "genus",
    "Species": "species",
}
PREFIX_TO_RANK = {
    "sk": "superkingdom",
    "d": "domain",
    "k": "kingdom",
    "sg": "supergroup",
    "p": "phylum",
    "dv": "division",
    "sdv": "subdivision",
    "c": "class",
    "o": "order",
    "f": "family",
    "g": "genus",
    "s": "species",
}


class ASVAnalysisInputFiles(BaseModel):
    """Resolved ASV result files for one analysis."""

    model_config = ConfigDict(frozen=True)

    fasta_files: list[Path] = Field(default_factory=list)
    reads_counts: list[Path] = Field(default_factory=list)
    taxonomy_files_by_database: dict[str, list[Path]] = Field(default_factory=dict)
    mapseq_files_by_database: dict[str, list[Path]] = Field(default_factory=dict)


@task
def build_amplicon_taxonomic_results_dataframe(
    mgnify_study_accession: str,
    analysis_ids: list[int] | None = None,
    pipeline_version: str = EMG_CONFIG.amplicon_pipeline.pipeline_version,
) -> pd.DataFrame:
    """Build a data frame of v6-family amplicon taxonomy rows from imported analyses.

    This task collects result files from ``Analysis.downloads`` before passing
    resolved input paths to the dataframe builders.

    :param mgnify_study_accession: MGnify study accession to summarize.
    :param analysis_ids: Optional analysis IDs to include.
    :param pipeline_version: Amplicon pipeline version to summarize.
    :return: Canonical taxonomy summary dataframe.
    """
    study = Study.objects.get(accession=mgnify_study_accession)
    # TODO: Some studies are quite large, this may timeout
    analyses = (
        Analysis.objects.filter(
            study=study,
            experiment_type=Analysis.ExperimentTypes.AMPLICON,
            pipeline_version__in=analysis_pipeline_versions_for_config(
                pipeline_version
            ),
        )
        .filter_by_statuses([Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED])
        .select_related("run", "sample", "sample__ena_sample", "study", "ena_study")
    )
    if analysis_ids is not None:
        analyses = analyses.filter(id__in=analysis_ids)

    output_frames: list[pd.DataFrame] = []

    for analysis in analyses:
        metadata = analysis_metadata(analysis, pipeline_version)
        output_frames.extend(
            build_asv_data_frames(analysis, metadata, collect_asv_input_files(analysis))
        )
        output_frames.extend(build_closed_reference_frames(analysis, metadata))

    return finalise_canonical_frames(output_frames)


@task()
def merge_amplicon_v6_taxonomy_summary_outputs(
    mgnify_study_accession: str,
    pipeline_version: str = EMG_CONFIG.amplicon_pipeline.pipeline_version,
    cleanup_partials: bool = False,
) -> Path | None:
    """Merge internal canonical partials and write public study summary outputs.

    The task writes one public parquet file for a study/pipeline-version pair.

    :param mgnify_study_accession: MGnify study accession to finalize.
    :param pipeline_version: Amplicon pipeline version to merge.
    :param cleanup_partials: Delete internal partial parquet files after writing
        public outputs.
    :return: Path to the public parquet output.
    """
    study = Study.objects.get(accession=mgnify_study_accession)

    output_dir = (
        study.results_dir_path
        / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{pipeline_version}"
    )
    partials_dir = output_dir / "summaries" / PARTIALS_DIRNAME
    partial_files = sorted(partials_dir.glob("*_taxonomy.parquet"))

    if not partial_files:
        logger.error(f"No amplicon taxonomy summary partials found in {partials_dir}")
        return

    frames = [pd.read_parquet(partial_file) for partial_file in partial_files]
    merged_df = ensure_canonical_columns(pd.concat(frames, ignore_index=True))
    output_dir.mkdir(parents=True, exist_ok=True)

    output_prefix = f"{study.accession}_amplicon_{pipeline_version}"
    parquet_path = output_dir / f"{output_prefix}{TAXONOMY_PARQUET_SUFFIX}"

    write_amplicon_taxonomy_summary_parquet(merged_df, parquet_path)
    if cleanup_partials:
        for partial_file in partial_files:
            partial_file.unlink()

    return parquet_path


@task
def add_amplicon_v6_taxonomy_summary_outputs_to_downloads(
    mgnify_study_accession: str,
    pipeline_version: str = EMG_CONFIG.amplicon_pipeline.pipeline_version,
) -> None:
    """Register parquet study outputs as downloads.

    :param mgnify_study_accession: MGnify study accession whose outputs should be
        registered.
    :param pipeline_version: Amplicon pipeline version used in filenames and
        download groups.
    """
    study = Study.objects.get(accession=mgnify_study_accession)

    output_dir = (
        study.results_dir_path
        / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{pipeline_version}"
    )
    output_prefix = f"{study.accession}_amplicon_{pipeline_version}"

    downloads: list[tuple[Path, DownloadFileType, str, str, str]] = [
        (
            output_dir / f"{output_prefix}{TAXONOMY_PARQUET_SUFFIX}",
            DownloadFileType.PARQUET,
            f"study_summary.{pipeline_version}.amplicon.parquet",
            "Amplicon taxonomic occurrence parquet summary",
            "Study-level amplicon taxonomic occurrence summary in parquet format.",
        ),
    ]

    for path, file_type, group, short_description, long_description in downloads:
        if not path.exists():
            raise FileNotFoundError(f"Missing amplicon taxonomy summary output: {path}")
        try:
            study.add_download(
                DownloadFile(
                    path=Path("study-summaries") / path.name,
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=group,
                    file_type=file_type,
                    short_description=short_description,
                    long_description=long_description,
                    alias=path.name,
                )
            )
        except FileExistsError:
            logger.warning("File %s already exists in downloads list, skipping", path)

    study.refresh_from_db()


def analysis_metadata(analysis: Analysis, pipeline_version: str) -> dict[str, object]:
    """Build canonical metadata fields for an analysis.

    :param analysis: Analysis model instance.
    :param pipeline_version: Amplicon pipeline version.
    :return: Metadata fields for canonical summary rows.
    """
    sample = analysis.sample
    sample_metadata = sample.metadata_preferring_inferred
    if sample.ena_sample:
        sample_metadata = {
            **sample.ena_sample.metadata,
            **sample_metadata,
        }

    flattened_sample_metadata_keys = {
        ENASampleFields.LON,
        ENASampleFields.LAT,
        ENASampleFields.DEPTH,
        ENASampleFields.TEMPERATURE,
        ENASampleFields.SALINITY,
        ENASampleFields.COLLECTION_DATE,
        ENASampleFields.COUNTRY,
        ENASampleFields.CENTER_NAME,
    }
    extra_sample_metadata = {}
    for key, value in sample_metadata.items():
        if key in flattened_sample_metadata_keys:
            continue
        # Preserve nested metadata values that parquet can represent.
        if isinstance(value, (dict, list, tuple, set)):
            if value:
                extra_sample_metadata[str(key)] = (
                    list(value) if isinstance(value, set) else value
                )
            continue

        value = normalise_empty(value)
        if pd.notna(value):
            extra_sample_metadata[str(key)] = value

    # Should we coerce data types here, for example longitude should be a float
    # the problem is that the metadata on ENA is not curated, so this would require
    # additional logic to handle different data types and units.

    return {
        "summary_schema_version": SUMMARY_SCHEMA_VERSION,
        "study_accession": analysis.study.accession,
        "ena_study_accession": analysis.ena_study.accession,
        "analysis_accession": analysis.accession,
        "pipeline_version": pipeline_version,
        "experiment_type": Analysis.ExperimentTypes.AMPLICON.value,
        "sample_accession": sample.first_accession,
        "run_accession": analysis.run.first_accession,
        # Keep ENA metadata values verbatim because they can include units or directions.
        "longitude": normalise_empty(sample_metadata.get(ENASampleFields.LON)),
        "latitude": normalise_empty(sample_metadata.get(ENASampleFields.LAT)),
        "depth": normalise_empty(sample_metadata.get(ENASampleFields.DEPTH)),
        "temperature": normalise_empty(
            sample_metadata.get(ENASampleFields.TEMPERATURE)
        ),
        "salinity": normalise_empty(sample_metadata.get(ENASampleFields.SALINITY)),
        "collection_date": normalise_empty(
            sample_metadata.get(ENASampleFields.COLLECTION_DATE)
        ),
        "country": normalise_empty(sample_metadata.get(ENASampleFields.COUNTRY)),
        "institution_code": normalise_empty(
            sample_metadata.get(ENASampleFields.CENTER_NAME)
        ),
        "sequencing_method": normalise_empty(analysis.run.instrument_model),
        "sample_metadata": extra_sample_metadata,
    }


def finalise_canonical_frames(output_frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Add public identifiers and enforce canonical output columns.

    :param output_frames: Canonical-ish summary frames to concatenate.
    :return: Canonical taxonomy summary dataframe.
    """
    if not output_frames:
        return ensure_canonical_columns(pd.DataFrame())

    canonical_df = pd.concat(output_frames, ignore_index=True)
    canonical_df["scientific_name"] = canonical_df.apply(deepest_taxon_name, axis=1)
    canonical_df["event_id"] = canonical_df.apply(
        lambda row: make_event_id(
            row["study_accession"],
            row["sample_accession"],
            row["run_accession"],
            row["pipeline_version"],
        ),
        axis=1,
    )
    canonical_df["event_guid"] = canonical_df.apply(
        lambda row: make_uuid(
            [
                "event",
                row["study_accession"],
                row["sample_accession"],
                row["run_accession"],
                row["pipeline_version"],
            ]
        ),
        axis=1,
    )
    canonical_df["occurrence_id"] = canonical_df.apply(
        lambda row: make_uuid(
            [
                "occurrence",
                row["analysis_accession"],
                row["pipeline_version"],
                row["analysis_method"],
                row["reference_database"],
                row["amplified_region"],
                row["asv_id"],
                row["scientific_name"],
                row["taxon_id"],
            ]
        ),
        axis=1,
    )
    return ensure_canonical_columns(canonical_df)


def analysis_pipeline_versions_for_config(
    pipeline_version: str,
) -> tuple[Analysis.PipelineVersions, ...]:
    """Map config pipeline version strings to Analysis model choices.

    :param pipeline_version: Config/public pipeline version string.
    :return: Analysis pipeline version choices to include in metadata queries.
    """
    if pipeline_version == "v6.1":
        return (Analysis.PipelineVersions.v6_1,)
    if pipeline_version == "v6":
        return (Analysis.PipelineVersions.v6,)
    return (Analysis.PipelineVersions.v6, Analysis.PipelineVersions.v6_1)


def collect_asv_input_files(analysis: Analysis) -> ASVAnalysisInputFiles:
    """Collect resolved ASV input files for one analysis.

    :param analysis: Analysis model instance.
    :return: Resolved ASV result file paths grouped by purpose.
    """
    fasta_files = _matching_download_paths(
        analysis,
        group=f"{Analysis.ASV}.sequences",
        file_type=DownloadFileType.FASTA,
    )
    reads_counts = _matching_download_paths(
        analysis,
        group=f"{Analysis.ASV}.distribution",
        suffix="_asv_read_counts.tsv",
    )

    return ASVAnalysisInputFiles(
        fasta_files=fasta_files,
        reads_counts=reads_counts,
        taxonomy_files_by_database={
            asv_database: _matching_download_paths(
                analysis,
                group="asv.distribution",
                suffix=f"_{asv_database}_asv_tax.tsv",
            )
            for asv_database, _ in ASV_DATABASES_AND_TAX_RANKS
        },
        mapseq_files_by_database={
            asv_database: _matching_download_paths(
                analysis,
                group=f"taxonomies.asv.{asv_database}",
                suffix=".mseq",
            )
            for asv_database, _ in ASV_DATABASES_AND_TAX_RANKS
        },
    )


def _matching_download_paths(
    analysis: Analysis,
    group: str | None = None,
    suffix: str | None = None,
    file_type: DownloadFileType | None = None,
) -> list[Path]:
    """Return matching downloads with resolved local paths."""
    downloads = sorted(
        analysis.matching_downloads(
            group=group,
            suffix=suffix,
            file_type=file_type,
        ),
        key=lambda dl: dl.alias,
    )

    # This is the NFS results directory
    analysis_results_dir = (
        Path(EMG_CONFIG.slurm.pipelines_root_dir)
        / Path(analysis.study.results_dir)
        / Path(analysis.results_dir)
    )
    download_paths = []
    for download in downloads:
        download_paths.append(analysis_results_dir / Path(download.path))

    return download_paths


# @task
def build_asv_data_frames_from_downloads(
    analysis: Analysis,
    metadata: dict[str, object],
) -> list[pd.DataFrame]:
    """Build ASV canonical frames from resolved analysis input files.

    Compatibility wrapper around ``collect_asv_input_files`` and
    ``build_asv_data_frames``.

    :param analysis: Analysis model instance.
    :param metadata: Canonical analysis and sample metadata.
    :return: Canonical ASV dataframes.
    """
    return build_asv_data_frames(analysis, metadata, collect_asv_input_files(analysis))


def build_asv_data_frames(
    analysis: Analysis,
    metadata: dict[str, object],
    input_files: ASVAnalysisInputFiles,
) -> list[pd.DataFrame]:
    """Build ASV canonical frames from resolved analysis input files.

    The frames contain fields from these ASV files:
    - FASTA files
    - Count files
    - MapSeq files

    :param analysis: Analysis model instance.
    :param metadata: Canonical analysis and sample metadata.
    :param input_files: Resolved ASV input files.
    :return: Canonical ASV dataframes.
    """

    data_frames: list[pd.DataFrame] = []

    asv_fasta_files = input_files.fasta_files
    if not asv_fasta_files:
        logger.warning(f"Analysis {analysis} missing ASV FASTA download")
        return data_frames

    if len(asv_fasta_files) > 1:
        raise ValueError(
            f"Analysis {analysis} has {len(asv_fasta_files)} ASV FASTA downloads; "
            "expected 1"
        )

    asv_fasta_file = asv_fasta_files[0]

    if not input_files.reads_counts:
        logger.warning(
            f"Analysis {analysis} missing ASV sequence or count downloads",
        )
        return data_frames

    # NOTE: this could be big for deeply sequenced samples
    fasta_df = pd.DataFrame(
        [
            {"asv": record.id, "asv_sequence": str(record.seq)}
            for record in SeqIO.parse(asv_fasta_file, "fasta")
        ]
    )
    if fasta_df.empty:
        logger.warning(
            f"Analysis {analysis} ASV FASTA download is empty",
        )
        return data_frames

    for asv_database, asv_tax_ranks in ASV_DATABASES_AND_TAX_RANKS:
        asv_tax_files = input_files.taxonomy_files_by_database.get(asv_database, [])
        mapseq_files = input_files.mapseq_files_by_database.get(asv_database, [])
        if not asv_tax_files or not mapseq_files:
            logger.warning(f"Analysis {analysis} missing ASV inputs for {asv_database}")
            continue

        if len(mapseq_files) > 1:
            logger.warning(
                f"Analysis {analysis} has multiple mapseq files for {asv_database}"
            )
            continue

        mapseq_df = _mapseq_to_df(mapseq_files[0])
        if mapseq_df.empty:
            logger.info(f"Analysis {analysis} mapseq file is empty for {asv_database}")
            continue

        for reads_count_file in input_files.reads_counts:
            if reads_count_file.stat().st_size == 0:
                logger.info(
                    f"Analysis {analysis} distribution count file is empty for {asv_database}"
                )
                continue

            # Expected structure:
            #   <analysis-results>/asv/<region>/<sample>_asv_read_counts.tsv
            # Older outputs can place the count file directly under:
            #   <analysis-results>/asv/<sample>_asv_read_counts.tsv
            amplified_region = reads_count_file.parent.name
            if amplified_region == "asv":
                amplified_region = reads_count_file.stem.removesuffix(
                    "_asv_read_counts"
                )

            asv_tax_file = asv_tax_file_for_amplified_region_and_asv_database(
                amplified_region,
                asv_tax_files,
                asv_database,
            )

            if asv_tax_file is None:
                logger.warning(
                    f"Analysis {analysis} missing {asv_database} ASV taxonomy file "
                    f"for {amplified_region}",
                )
                continue

            run_tax_df = _normalise_rank_dataframe(
                pd.read_csv(asv_tax_file, sep="\t"), asv_tax_ranks
            )
            count_df = pd.read_csv(reads_count_file, sep="\t")

            merged_df = count_df.merge(
                run_tax_df,
                left_on="asv",
                right_on="ASV",
                validate="one_to_one",
            )
            merged_df = merged_df.merge(mapseq_df, on="asv", validate="one_to_one")
            merged_df = merged_df.merge(fasta_df, on="asv", validate="one_to_one")
            if len(merged_df) != len(count_df):
                logger.error(
                    f"Analysis {analysis,} kept {len(merged_df)} of {len(count_df)} ASV count rows for {asv_database} {amplified_region} after joins",
                )

            merged_df["taxon_id"] = _generate_asv_taxids(
                merged_df,
                asv_tax_file,
            )

            data_frames.append(
                _asv_to_canonical(
                    merged_df,
                    metadata,
                    asv_database,
                    amplified_region,
                )
            )
    return data_frames


def build_closed_reference_frames(
    analysis: Analysis,
    metadata: dict[str, object],
) -> list[pd.DataFrame]:
    """Build closed-reference canonical frames from registered downloads.

    :param analysis: Analysis model instance.
    :param metadata: Canonical analysis and sample metadata.
    :return: Canonical closed-reference dataframes.
    """
    data_frames: list[pd.DataFrame] = []
    for database_name in CLOSED_REFERENCE_DATABASES:
        tsv_files = _matching_download_paths(
            analysis,
            group=f"taxonomies.closed_reference.{database_name}",
            suffix=".tsv",
        )
        if not tsv_files:
            logger.info(
                f"Analysis {analysis} missing closed-reference TSV for "
                f"{database_name}",
            )
            continue

        if len(tsv_files) > 1:
            raise ValueError(
                f"Analysis {analysis} has {len(tsv_files)} TSV files for "
                f"{database_name}; expected 1"
            )

        tsv_file = tsv_files[0]
        tax_df = read_closed_reference_tsv_file(tsv_file)
        if tax_df.empty:
            continue

        data_frames.append(
            _closed_reference_to_canonical(tax_df, metadata, database_name)
        )

    return data_frames


def asv_tax_file_for_amplified_region_and_asv_database(
    amplified_region: str,
    asv_tax_files: list[Path],
    asv_database: str,
) -> Path | None:
    """Find the ASV taxonomy file matching a count file's amplified region.

    Older outputs have one taxonomy file per database. Multi-region outputs can
    have one taxonomy file per database and region, so this matches by region
    first and only falls back to a single unambiguous file.

    :param amplified_region: Amplified region label.
    :param asv_tax_files: Candidate ASV taxonomy files.
    :param asv_database: ASV reference database name.
    :return: Matching taxonomy file, or ``None``.
    """
    if len(asv_tax_files) == 1:
        return asv_tax_files[0]

    # Match the files that correspond to the count file's amplified region.
    matching_files = [
        tax_file
        for tax_file in asv_tax_files
        if asv_database in tax_file.name
        and (amplified_region in tax_file.parts or amplified_region in tax_file.stem)
    ]

    if len(matching_files) > 1:
        raise ValueError(f"Multiple matching taxonomy files: {matching_files}")

    return matching_files[0] if matching_files else None


def _mapseq_to_df(mapseq_file: Path) -> pd.DataFrame:
    """
    Transform the mapseq file into a dataframe.

    :param mapseq_file: mapseq file.
    :return: Dataframe with ASV hit fields, or an empty dataframe.
    """
    if mapseq_file.stat().st_size == 0:
        return pd.DataFrame()

    mapseq_df = pd.read_csv(
        mapseq_file,
        sep="\t",
        comment="#",
        header=None,
        usecols=[0, 1, 3, 9, 10],
    )
    mapseq_df.columns = [
        "asv",
        "dbhit",
        "dbhit_identity",
        "dbhit_start",
        "dbhit_end",
    ]
    return mapseq_df


def _normalise_rank_dataframe(df: pd.DataFrame, ranks: list[str]) -> pd.DataFrame:
    """Add canonical snake_case taxonomy rank columns to a result dataframe.

    :param df: Pipeline taxonomy dataframe with version/native rank columns.
    :param ranks: Ordered native rank columns expected for the reference database.
    :return: Copy of ``df`` with canonical rank columns added.
    """
    normalised = df.copy()
    for rank in ranks:
        if rank not in normalised.columns:
            normalised[rank] = pd.NA
        normalised[RANK_COLUMN_MAP[rank]] = normalised[rank].apply(strip_taxon_prefix)
    return normalised


def read_closed_reference_tsv_file(tsv_file: Path) -> pd.DataFrame:
    """Read one registered closed-reference taxonomy TSV.

    :param tsv_file: Closed-reference TSV download path.
    :return: Dataframe with taxonomy columns and taxids, or an empty dataframe.
    """
    if tsv_file.stat().st_size == 0:
        return pd.DataFrame()

    tsv_df = pd.read_csv(
        tsv_file,
        sep="\t",
        comment="#",
        header=None,
        names=["otu_id", "measurement_value", "taxonomy_path", "taxon_id"],
    )

    # Remove non-comment taxonomy TSV header rows read as data
    header_mask = tsv_df.apply(_is_closed_reference_tsv_header_row, axis=1)
    tsv_df = tsv_df.loc[~header_mask].copy()

    if tsv_df.empty:
        return pd.DataFrame()

    tsv_df["measurement_value"] = pd.to_numeric(
        tsv_df["measurement_value"],
        errors="coerce",
    )
    return _taxonomy_path_to_columns(tsv_df)


def _is_closed_reference_tsv_header_row(row: pd.Series) -> bool:
    """Detect a closed-reference taxonomy TSV header row.

    :param row: Raw TSV row read with fixed columns.
    :return: Whether the row is a non-comment header.
    """
    normalise_header_cell = lambda value: (
        "" if pd.isna(value) else str(value).strip().lower()
    )

    return (
        normalise_header_cell(row.get("otu_id")) in {"otu id", "otu_id"}
        and normalise_header_cell(row.get("taxonomy_path")) == "taxonomy"
        and normalise_header_cell(row.get("taxon_id")) == "taxid"
    )


def _taxonomy_path_to_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Expand semicolon-delimited prefixed taxonomy paths into rank columns.

    :param df: Dataframe containing a ``taxonomy_path`` column.
    :return: Copy of ``df`` with canonical rank columns and coerced taxids.
    """
    expanded = df.copy()
    for column in TAXONOMY_COLUMNS:
        expanded[column] = pd.NA

    for index, taxonomy_path in expanded["taxonomy_path"].items():
        for taxon in str(taxonomy_path).split(";"):
            if "__" not in taxon:
                continue
            prefix, _, value = taxon.partition("__")
            rank = PREFIX_TO_RANK.get(prefix)
            if rank:
                expanded.at[index, rank] = normalise_empty(value)

    expanded["taxon_id"] = expanded["taxon_id"].apply(coerce_taxon_id)
    return expanded


def _generate_asv_taxids(df: pd.DataFrame, asv_tax_file: Path) -> list[object]:
    """Look up ASV taxids from the registered ASV taxonomy download.

    :param df: ASV dataframe before taxonomy prefix cleanup.
    :param asv_tax_file: Registered ASV taxonomy TSV download.
    :return: Taxid values aligned to ``df`` rows.
    """
    tax_df = pd.read_csv(asv_tax_file, sep="\t")
    taxid_column = taxid_column_name(tax_df)
    if taxid_column is None:
        return [pd.NA] * len(df)

    taxid_by_asv = {
        row["ASV"]: coerce_taxon_id(row[taxid_column]) for _, row in tax_df.iterrows()
    }
    return [taxid_by_asv.get(asv, pd.NA) for asv in df["asv"]]


def taxid_column_name(df: pd.DataFrame) -> str | None:
    """Return the first taxid-like column name from a dataframe.

    :param df: Dataframe to inspect.
    :return: Matching taxid column name, or ``None``.
    """
    for column in df.columns:
        normalised_column = (
            str(column).strip().lower().replace("_", "").replace(" ", "")
        )
        if normalised_column in {"taxid", "taxonid", "ncb-taxid", "ncbitaxid"}:
            return column
    return None


def _asv_to_canonical(
    df: pd.DataFrame,
    metadata: dict[str, object],
    db: str,
    amplified_region: str,
) -> pd.DataFrame:
    """Convert merged ASV result fields to canonical summary columns.

    :param df: Merged ASV count, taxonomy, MAPseq, sequence, and taxid dataframe.
    :param metadata: Canonical run metadata fields.
    :param db: DADA2 reference database name.
    :param amplified_region: Amplified region represented by the ASV counts.
    :return: Canonical ASV summary dataframe.
    """
    canonical = pd.DataFrame(index=df.index)
    for key, value in metadata.items():
        canonical[key] = [value] * len(canonical.index)
    for column in TAXONOMY_COLUMNS:
        canonical[column] = df[column] if column in df.columns else pd.NA
    canonical["analysis_method"] = "asv"
    canonical["reference_database"] = db
    canonical["amplified_region"] = amplified_region
    canonical["taxon_id"] = df["taxon_id"].apply(coerce_taxon_id)
    canonical["measurement_value"] = df["count"]
    canonical["measurement_unit"] = "DNA sequence reads"
    canonical["asv_id"] = df["asv"]
    canonical["asv_sequence"] = df["asv_sequence"]
    canonical["asv_caller"] = "DADA2"
    canonical["dbhit"] = df["dbhit"]
    canonical["dbhit_identity"] = df["dbhit_identity"]
    canonical["dbhit_start"] = df["dbhit_start"]
    canonical["dbhit_end"] = df["dbhit_end"]
    return canonical


def _closed_reference_to_canonical(
    df: pd.DataFrame,
    metadata: dict[str, object],
    db: str,
) -> pd.DataFrame:
    """Convert closed-reference result fields to canonical summary columns.

    :param df: Closed-reference measurement, taxonomy, and taxid dataframe.
    :param metadata: Canonical run metadata fields.
    :param db: Closed-reference database name.
    :return: Canonical closed-reference summary dataframe.
    """
    canonical = pd.DataFrame(index=df.index)
    for key, value in metadata.items():
        canonical[key] = [value] * len(canonical.index)
    for column in TAXONOMY_COLUMNS:
        canonical[column] = df[column] if column in df.columns else pd.NA
    canonical["analysis_method"] = "closed_reference"
    canonical["reference_database"] = db
    canonical["amplified_region"] = pd.NA
    canonical["taxon_id"] = df["taxon_id"].apply(coerce_taxon_id)
    canonical["measurement_value"] = df["measurement_value"]
    canonical["measurement_unit"] = "DNA sequence reads"
    return canonical


@flow
def generate_study_summary_parquet(
    mgnify_study_accession: str,
    pipeline_version: str = EMG_CONFIG.amplicon_pipeline.pipeline_version,
    output_name: str | None = None,
) -> Path:
    """Generate a v6-family amplicon taxonomy parquet summary for a whole study.

    :param mgnify_study_accession: MGnify study accession to summarize.
    :param pipeline_version: Amplicon pipeline version to summarize.
    :param output_name: Optional output filename stem.
    :return: Path to the written parquet file.
    """
    # TODO: add support for any kind of study accession (MGYS and ENA ones)
    study = Study.objects.get(accession=mgnify_study_accession)

    summary_dir = (
        study.results_dir_path
        / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{pipeline_version}"
        / "summaries"
    )
    if not summary_dir.exists():
        raise FileNotFoundError(
            f"No summary directory found for {study}. Expected: {summary_dir}"
        )

    logger.info(
        f"Generating amplicon {pipeline_version} taxonomic parquet summary for {study} from imported analyses",
    )
    final_df = build_amplicon_taxonomic_results_dataframe(
        mgnify_study_accession=study.accession,
        pipeline_version=pipeline_version,
    )

    output_path = summary_dir / f"{output_name or study.accession}_taxonomy.parquet"

    return write_amplicon_taxonomy_summary_parquet(final_df, output_path)
