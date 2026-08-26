import logging
import re

import httpx

from analyses.models import Analysis, Biome, Study
from workflows.flows.data_dumps.ebi_search.utils.functional_annotations import (
    functional_cross_references,
)
from workflows.flows.data_dumps.ebi_search.utils.taxonomic_annotations import (
    taxonomy_lineages,
)
from workflows.flows.data_dumps.ebi_search.utils.xml import (
    EBISearchDate,
    EBISearchEntry,
    EBISearchField,
    EBISearchHierarchy,
    field,
    hierarchy,
    reference,
    value,
)

SAMPLE_METADATA_FIELDS = (
    ("temperature", ("temperature",)),
    ("pH", ("ph", "pH")),
    ("altitude", ("altitude",)),
    ("depth", ("depth",)),
    ("elevation", ("elevation",)),
    ("salinity", ("salinity",)),
    ("sequencing_method", ("sequencing_method",)),
    ("location_name", ("location", "country")),
    ("disease_status", ("disease_status", "disease")),
    ("phenotype", ("phenotype", "host_phenotype")),
)


def _date(value_: object) -> str:
    if value_ is None:
        return ""
    if hasattr(value_, "date"):
        value_ = value_.date()
    return str(value_)[:10]


def _pipeline_release(analysis: Analysis) -> str:
    return analysis.get_pipeline_version_display().removeprefix("v")


def analysis_document_id(analysis: Analysis) -> str:
    return f"{analysis.accession}_{_pipeline_release(analysis)}"


def _accession(accessions: list[str] | None, pattern: str) -> str | None:
    matcher = re.compile(pattern)
    return next((item for item in accessions or [] if matcher.fullmatch(item)), None)


def biome_lineages() -> dict[int, list[str]]:
    biomes = list(Biome.objects.values("id", "path", "biome_name"))
    names_by_path = {str(biome["path"]): biome["biome_name"] for biome in biomes}
    lineages = {}
    for biome in biomes:
        path_parts = str(biome["path"]).split(".")
        lineage = [
            names_by_path[path]
            for index in range(1, len(path_parts) + 1)
            if (path := ".".join(path_parts[:index])) in names_by_path
        ]
        if lineage and lineage[0].lower() == "root":
            lineage = lineage[1:]
        lineages[biome["id"]] = lineage or ["root"]
    return lineages


def _biome_lineage(study: Study, lineages: dict[int, list[str]]) -> list[str]:
    if not study.biome_id:
        return ["root"]
    return lineages.get(study.biome_id, [study.biome.biome_name])


def _first_metadata_value(metadata: dict, keys: tuple[str, ...]) -> object | None:
    for key in keys:
        value_ = metadata.get(key)
        if value_ is not None and str(value_) != "":
            return value_
    return None


def _sample_metadata_fields(analysis: Analysis) -> list[tuple[str, object]]:
    metadata = analysis.sample.metadata_preferring_inferred
    fields = []
    for output_name, metadata_names in SAMPLE_METADATA_FIELDS:
        if (value_ := _first_metadata_value(metadata, metadata_names)) is not None:
            fields.append((output_name, value_))
    return fields


def project_entry(
    study: Study, biome_lineages_: dict[int, list[str]]
) -> EBISearchEntry:
    metadata = study.metadata_preferring_inferred
    name = metadata.get("study_name") or study.title
    description = metadata.get("study_description") or metadata.get("description", "")
    secondary_accession = metadata.get("secondary_study_accession") or _accession(
        study.ena_accessions, r"[EDS]RP\d+"
    )
    project_accession = _accession(study.ena_accessions, r"PRJ[NED][AB]\d+")

    references = []
    if project_accession:
        references.append(reference("ena_project", project_accession))
    references.extend(
        reference("metagenomics_analyses", analysis_document_id(analysis))
        for analysis in study.ebi_search_analyses
    )
    analysis_fields = [
        field("experiment_type", experiment_type)
        for experiment_type in sorted(
            {
                analysis.get_experiment_type_display().lower()
                for analysis in study.ebi_search_analyses
            }
        )
    ]
    analysis_fields.extend(
        field("pipeline_version", pipeline_version)
        for pipeline_version in sorted(
            {_pipeline_release(analysis) for analysis in study.ebi_search_analyses}
        )
    )

    return EBISearchEntry(
        identifier=study.accession,
        name=value(name),
        description=value(description),
        dates=[
            EBISearchDate(
                type="creation_date",
                value=_date(metadata.get("first_public") or study.created_at),
            ),
            EBISearchDate(
                type="last_modification_date",
                value=_date(metadata.get("last_updated") or study.updated_at),
            ),
        ],
        additional_fields=[
            *analysis_fields,
            field("secondary_acc", secondary_accession),
            field("biome_name", study.biome.biome_name if study.biome else "root"),
            hierarchy("biome", _biome_lineage(study, biome_lineages_)),
            field("centre_name", metadata.get("center_name")),
        ],
        cross_references=references,
    )


def analysis_entry(
    analysis: Analysis,
    transfer_services_url_root: str,
    client: httpx.Client,
    run_logger: logging.Logger,
    biome_lineages_: dict[int, list[str]],
) -> EBISearchEntry:
    sample_metadata = analysis.sample.metadata_preferring_inferred
    study_metadata = analysis.study.metadata_preferring_inferred
    project_name = study_metadata.get("study_name") or analysis.study.title
    project_accession = _accession(analysis.study.ena_accessions, r"PRJ[NED][AB]\d+")

    fields: list[EBISearchField | EBISearchHierarchy] = [
        field("experiment_type", analysis.get_experiment_type_display().lower()),
        field("pipeline_version", _pipeline_release(analysis)),
        field("sample_name", sample_metadata.get("sample_title")),
        field("sample_description", sample_metadata.get("sample_description")),
        field("project_name", project_name),
        field(
            "biome_name",
            analysis.study.biome.biome_name if analysis.study.biome else "root",
        ),
    ]
    for output_name, metadata_name in (
        ("species", "scientific_name"),
        ("feature", "environment_feature"),
        ("material", "environment_material"),
    ):
        if value_ := sample_metadata.get(metadata_name):
            fields.append(field(output_name, value_))
    fields.append(field("sample_alias", sample_metadata.get("sample_alias")))
    # Kept twice for compatibility with the legacy dump template.
    fields.append(field("project_name", project_name))
    fields.append(hierarchy("biome", _biome_lineage(analysis.study, biome_lineages_)))
    for name, value_ in _sample_metadata_fields(analysis):
        fields.append(field(name, value_))
    for lineage in taxonomy_lineages(analysis):
        fields.append(hierarchy("organism", lineage))

    references = [reference("metagenomics_projects", analysis.study.accession)]
    if project_accession:
        references.append(reference("ena_project", project_accession))
    if biosample_accession := _accession(
        analysis.sample.ena_accessions, r"SAM[NED][AG]?\d+"
    ):
        references.append(reference("biosamples", biosample_accession))
    if insdc_sample_accession := _accession(
        analysis.sample.ena_accessions, r"[EDS]RS\d+"
    ):
        references.append(reference("sra-sample", insdc_sample_accession))
    if analysis.assembly:
        references.append(reference("analysis", analysis.assembly.first_accession))
    if analysis.run:
        references.append(reference("ena_run", analysis.run.first_accession))

    functional_references = functional_cross_references(
        analysis, transfer_services_url_root, client, run_logger
    )
    for database_name in ("go", "interpro", "rhea"):
        for identifier in sorted(functional_references[database_name]):
            references.append(reference(database_name, identifier))

    return EBISearchEntry(
        identifier=analysis_document_id(analysis),
        name=value(analysis.accession),
        dates=[
            EBISearchDate(type="creation_date", value=_date(analysis.created_at)),
            EBISearchDate(type="completion_date", value=_date(analysis.updated_at)),
            EBISearchDate(
                type="sample_collection_date",
                value=_date(sample_metadata.get("collection_date")),
            ),
        ],
        additional_fields=fields,
        cross_references=references,
    )
