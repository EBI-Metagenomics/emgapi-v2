"""Import viral catalogue representatives and provenance through Prefect."""

import csv
import gzip
import json
import math
import re
from collections import defaultdict
from pathlib import Path

from django.core.exceptions import ValidationError
from django.db import transaction

import activate_django_first  # noqa: F401

from analyses.models import Analysis
from genomes.models import CatalogueGenome
from viruses.downloads import catalogue_downloads
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment, ViralFragment
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task

REQUIRED_COLUMNS = {
    "ID",
    "Original_ID",
    "Cluster_rep",
    "Source",
    "Source_accession",
    "Sequence_length",
    "Sequence_sha256",
}
MISSING = {"", "NA", "N/A", "missing", "None", "-"}
ANNOTATION_FIELDS = (
    "length",
    "prediction_tool",
    "checkv_quality",
    "miuvig_quality",
    "completeness",
    "contamination",
    "is_provirus",
    "sequence_type",
    "taxonomy",
)


def optional_number(value: str) -> float | None:
    """Parse an optional finite percentage.

    :param value: The TSV value.
    :return: A percentage or None.
    :raises ValueError: If the percentage is invalid.
    """
    if value in MISSING:
        return None
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 100:
        raise ValueError(f"Invalid percentage: {value}")
    return number


def read_fragments(directory: Path, kind: str = "viruses") -> list[dict]:
    """Validate all detections, including repeated accessions at different sources.

    :param directory: The catalogue root.
    :return: Validated source rows and their provenance snapshots.
    :raises ValueError: If identifiers, annotations or cluster references conflict.
    """
    compressed = directory / f"{kind}/{kind}-all-metadata.tsv.gz"
    path = compressed if compressed.exists() else compressed.with_suffix("")
    opener = gzip.open if path.suffix == ".gz" else open
    rows = []
    identities = {}
    genomes = {}
    analyses = {}
    with opener(path, "rt", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not REQUIRED_COLUMNS.issubset(reader.fieldnames or []):
            raise ValueError(
                f"Missing viral metadata columns: {REQUIRED_COLUMNS - set(reader.fieldnames or [])}"
            )
        for source in reader:
            accession = source["ID"]
            checksum = source["Sequence_sha256"]
            checksum = "" if checksum in MISSING else checksum
            if not re.fullmatch(r"MGYV\d{10}", accession):
                raise ValueError(f"Invalid fragment accession: {accession}")
            if checksum and not re.fullmatch(r"[a-f0-9]{64}", checksum):
                raise ValueError(f"Invalid sequence checksum for {accession}")
            identity = (checksum, source["Cluster_rep"])
            if accession in identities and identities[accession] != identity:
                raise ValueError(f"Conflicting sequence or cluster for {accession}")
            identities[accession] = identity
            source_type = (
                "analysis" if source["Source"] == "assembly" else source["Source"]
            )
            if source_type not in {"genome", "analysis"}:
                raise ValueError(f"Unknown fragment source: {source['Source']}")
            if source["Source_accession"] in MISSING:
                raise ValueError(f"Missing source accession for {accession}")
            genome_accession = None
            genome = None
            analysis = None
            if source_type == "genome":
                candidate = (
                    source.get("Source_genome")
                    or source["Original_ID"].split("_", 1)[0]
                )
                genome_accession = (
                    candidate if re.fullmatch(r"MGYG\d+", candidate) else None
                )
                explicit_id = source.get("Source_catalogue_genome")
                catalogue_id = source.get("Source_genome_catalogue")
                key = (genome_accession, explicit_id, catalogue_id)
                if key not in genomes:
                    queryset = CatalogueGenome.objects.select_related(
                        "genome", "catalogue"
                    )
                    if explicit_id:
                        linked = queryset.get(pk=int(explicit_id))
                        if (
                            genome_accession
                            and linked.genome.accession != genome_accession
                        ):
                            raise ValueError(
                                "The catalogue genome does not match the source genome"
                            )
                        genomes[key] = linked
                    elif catalogue_id:
                        genomes[key] = queryset.get(
                            genome__accession=genome_accession,
                            catalogue_id=catalogue_id,
                        )
                    else:
                        genomes[key] = queryset.filter(
                            genome__accession=genome_accession,
                            catalogue__status="published",
                        ).first()
                genome = genomes[key]
                if genome:
                    genome_accession = genome.genome.accession
            else:
                analysis_accession = source.get("Source_analysis", "")
                if not analysis_accession and source["Source_accession"].startswith(
                    "MGYA"
                ):
                    analysis_accession = source["Source_accession"]
                if analysis_accession:
                    if analysis_accession not in analyses:
                        analyses[analysis_accession] = Analysis.objects.get(
                            accession=analysis_accession
                        )
                    analysis = analyses[analysis_accession]
                    if not analysis.assembly_id:
                        raise ValueError(
                            f"Source {analysis_accession} is not an assembly analysis"
                        )
            links = {
                "catalogue_genome_id": genome.pk if genome else None,
                "analysis_id": analysis.pk if analysis else None,
            }
            if source.get("provirus", "No") not in {"Yes", "No"}:
                raise ValueError(f"Invalid provirus value for {accession}")
            length = int(source["Sequence_length"])
            if length <= 0:
                raise ValueError(f"Invalid sequence length for {accession}")
            sequence_type = (
                "plasmid" if kind == "plasmids" else source.get("Sequence_type")
            ) or ("prophage" if "|prophage-" in source["Original_ID"] else "virus")
            if sequence_type not in {"virus", "prophage", "plasmid"}:
                raise ValueError(f"Invalid fragment type for {accession}")
            rows.append(
                {
                    "accession": accession,
                    "nt_sequence_sha256": checksum,
                    "cluster_representative": source["Cluster_rep"],
                    "length": length,
                    "prediction_tool": source.get("Source_of_prediction", ""),
                    "checkv_quality": source.get("checkv_quality", ""),
                    "miuvig_quality": source.get("miuvig_quality", ""),
                    "completeness": optional_number(source.get("completeness", "")),
                    "contamination": optional_number(source.get("contamination", "")),
                    "is_provirus": source.get("provirus", "No") == "Yes",
                    "sequence_type": sequence_type,
                    "taxonomy": {
                        key: source.get(f"{key}_lineage", "")
                        for key in ("vitap", "viphogs", "genomad")
                    },
                    "metadata": source,
                    "links": links,
                }
            )
    if not rows:
        raise ValueError("The viral catalogue contains no fragments")
    representatives = {
        row["accession"]
        for row in rows
        if row["accession"] == row["cluster_representative"]
    }
    if missing := {row["cluster_representative"] for row in rows} - representatives:
        raise ValueError(
            f"Missing or invalid cluster representatives: {sorted(missing)}"
        )
    return rows


def group_fragments(rows: list[dict]) -> list[dict]:
    """Keep representative summaries and the original rows of each cluster.

    :param rows: Validated detections from the metadata table.
    :return: One entry payload per representative.
    :raises ValueError: If annotations disagree between representative detections.
    """
    groups = defaultdict(list)
    for row in rows:
        groups[row["cluster_representative"]].append(row)
    entries = []
    for accession, detections in groups.items():
        representatives = [row for row in detections if row["accession"] == accession]
        representative = representatives[0]
        if any(
            any(
                row[field] != representative[field]
                for field in ANNOTATION_FIELDS
                if field != "prediction_tool"
            )
            for row in representatives[1:]
        ):
            raise ValueError(f"Conflicting representative annotations for {accession}")
        metadata_rows = [row["metadata"] for row in detections]
        entries.append(
            {
                **{field: representative[field] for field in ANNOTATION_FIELDS},
                "accession": accession,
                "prediction_tool": ";".join(
                    sorted(
                        {
                            row["prediction_tool"]
                            for row in representatives
                            if row["prediction_tool"]
                        }
                    )
                ),
                "nt_sequence_sha256": representative["nt_sequence_sha256"],
                "metadata_rows": metadata_rows,
                "analysis_ids": {
                    row["links"]["analysis_id"]
                    for row in detections
                    if row["links"]["analysis_id"]
                },
                "genome_ids": {
                    row["links"]["catalogue_genome_id"]
                    for row in detections
                    if row["links"]["catalogue_genome_id"]
                },
                "cluster_size": len({row["accession"] for row in detections}),
            }
        )
    return entries


@task(name="Import viral catalogue", persist_result=False)
@transaction.atomic
def import_catalogue(
    results_directory: str,
    catalogue_id: str,
    name: str,
    version: str,
    biome: str,
    ftp_url: str,
    description: str = "",
    pipeline_version: str = "",
    is_public: bool = False,
) -> ViralCatalogue:
    """Import representative snapshots without changing other catalogue releases.

    :param results_directory: The catalogue root directory.
    :param catalogue_id: The release slug.
    :param name: The catalogue display name.
    :param version: The catalogue release version.
    :param biome: The biome label.
    :param ftp_url: The public download directory.
    :param description: The catalogue description.
    :param pipeline_version: The pipeline version.
    :param is_public: Whether to expose the imported release.
    :return: The imported catalogue.
    :raises ValidationError: If a source is private or a stable identity conflicts.
    """
    directory = Path(results_directory)
    rows = read_fragments(directory)
    if (directory / "plasmids/plasmids-all-metadata.tsv.gz").exists() or (
        directory / "plasmids/plasmids-all-metadata.tsv"
    ).exists():
        rows += read_fragments(directory, "plasmids")
    payloads = group_fragments(rows)
    if is_public:
        source_links = [row["links"] for row in rows]
        analysis_ids = {
            item["analysis_id"] for item in source_links if item["analysis_id"]
        }
        genome_ids = {
            item["catalogue_genome_id"]
            for item in source_links
            if item["catalogue_genome_id"]
        }
        if Analysis.public_objects.filter(pk__in=analysis_ids).count() != len(
            analysis_ids
        ):
            raise ValidationError(
                "A public catalogue cannot contain a private, suppressed or incomplete analysis"
            )
        if CatalogueGenome.objects.filter(
            pk__in=genome_ids, catalogue__status__in=["published", "retired"]
        ).count() != len(genome_ids):
            raise ValidationError(
                "A public catalogue cannot contain an unpublished catalogue genome"
            )
    summary = directory / "catalogue.json"
    catalogue, _ = ViralCatalogue.objects.get_or_create(
        catalogue_id=catalogue_id,
        defaults={"name": name, "version": version, "biome": biome, "ftp_url": ftp_url},
    )
    catalogue = ViralCatalogue.objects.select_for_update().get(pk=catalogue.pk)
    for key, value in {
        "name": name,
        "version": version,
        "biome": biome,
        "ftp_url": ftp_url,
        "description": description,
        "pipeline_version": pipeline_version,
        "is_public": is_public,
        "other_stats": json.loads(summary.read_text()) if summary.exists() else {},
        "downloads": catalogue_downloads(directory),
    }.items():
        setattr(catalogue, key, value)
    if is_public:
        ViralCatalogue.objects.exclude(pk=catalogue.pk).filter(is_public=True).update(
            is_public=False
        )
    catalogue.full_clean()
    catalogue.save()
    catalogue.fragment_entries.all().delete()
    for payload in payloads:
        analysis_ids = payload.pop("analysis_ids")
        genome_ids = payload.pop("genome_ids")
        accession = payload.pop("accession")
        checksum = payload.pop("nt_sequence_sha256")
        fragment, _ = ViralFragment.objects.get_or_create(
            accession=accession, defaults={"nt_sequence_sha256": checksum}
        )
        if (
            fragment.nt_sequence_sha256
            and checksum
            and fragment.nt_sequence_sha256 != checksum
        ):
            raise ValidationError(
                f"Conflicting nucleotide checksum for stable fragment {accession}"
            )
        if not fragment.nt_sequence_sha256 and checksum:
            fragment.nt_sequence_sha256 = checksum
            fragment.save(update_fields=["nt_sequence_sha256", "updated_at"])
        entry = ViralCatalogueViralFragment(
            catalogue=catalogue, fragment=fragment, **payload
        )
        entry.full_clean(validate_unique=False, validate_constraints=False)
        entry.save()
        entry.source_analyses.set(analysis_ids)
        entry.source_genomes.set(genome_ids)
    return catalogue


@flow(name="import_viral_catalogue_flow")
def import_viral_catalogue_flow(
    results_directory: str,
    catalogue_id: str,
    name: str,
    version: str,
    biome: str,
    ftp_url: str,
    description: str = "",
    pipeline_version: str = "",
    is_public: bool = False,
) -> str:
    """Import local viral metadata in one database transaction.

    :param results_directory: The catalogue root with its viruses metadata TSV.
    :param catalogue_id: The catalogue release slug.
    :param name: The catalogue display name.
    :param version: The catalogue release version.
    :param biome: The catalogue biome label.
    :param ftp_url: The public catalogue download directory.
    :param description: The catalogue description.
    :param pipeline_version: The VIRify pipeline version.
    :param is_public: Whether to expose the catalogue through the public API.
    :return: The imported catalogue ID.
    """
    catalogue = import_catalogue(
        results_directory=results_directory,
        catalogue_id=catalogue_id,
        name=name,
        version=version,
        biome=biome,
        ftp_url=ftp_url,
        description=description,
        pipeline_version=pipeline_version,
        is_public=is_public,
    )
    return catalogue.pk
