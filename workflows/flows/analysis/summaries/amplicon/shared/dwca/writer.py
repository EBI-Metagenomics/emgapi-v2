from __future__ import annotations

import csv
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin
from xml.etree import ElementTree

import pandas as pd

from activate_django_first import EMG_CONFIG

from analyses.models import Study
from workflows.data_io_utils.darwin_core.dwca_models import (
    GBIF_GMP_XSD,
    Abstract,
    Contact,
    Creator,
    Dataset,
    Distribution,
    DistributionOnline,
    Eml,
    IntellectualRights,
    KeywordSet,
    License,
    MetadataProvider,
    Methods,
    MethodStep,
    Sampling,
)
from workflows.ena_utils.study import ENAStudyFields
from workflows.flows.analysis.summaries.amplicon.shared.schema import (
    ensure_canonical_columns,
)

OCCURRENCE_COLUMNS = [
    "occurrenceID",
    "eventID",
    "eventDate",
    "country",
    "decimalLatitude",
    "decimalLongitude",
    "verbatimDepth",
    "institutionCode",
    "basisOfRecord",
    "scientificName",
    "taxonID",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "specificEpithet",
    "organismQuantity",
    "organismQuantityType",
    "identifiedBy",
    "identificationReferences",
]
DWCA_TEXT_NAMESPACE = "http://rs.tdwg.org/dwc/text/"
DWCA_TERM_NAMESPACE = "http://rs.tdwg.org/dwc/terms/"


def write_amplicon_taxonomy_dwca_archive(
    df: pd.DataFrame,
    study: Study,
    output_path: Path,
    analysis_method: str,
    reference_database: str | None = None,
    amplified_region: str | None = None,
) -> Path:
    """Write an Occurrence-core Darwin Core Archive for one product grouping.

    :param df: Canonical amplicon taxonomy dataframe containing one or more methods.
    :param study: Study used to populate EML metadata.
    :param output_path: Destination ``.dwca.zip`` path.
    :param analysis_method: Canonical method to include, such as ``asv`` or
        ``closed_reference``.
    :param reference_database: Optional reference database filter.
    :param amplified_region: Optional amplified-region filter for ASV archives.
    :return: Path to the written Darwin Core Archive.
    """
    archive_df = ensure_canonical_columns(df)
    archive_df = archive_df.loc[archive_df["analysis_method"] == analysis_method]
    if reference_database is not None:
        archive_df = archive_df.loc[
            archive_df["reference_database"] == reference_database
        ]
    if amplified_region is not None:
        archive_df = archive_df.loc[archive_df["amplified_region"] == amplified_region]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        _write_occurrence(archive_df, tmpdir / "occurrence.txt")
        _write_meta_xml(tmpdir / "meta.xml")
        _write_eml_xml(
            study,
            archive_df,
            analysis_method,
            reference_database,
            amplified_region,
            tmpdir / "eml.xml",
        )

        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for member in ("eml.xml", "meta.xml", "occurrence.txt"):
                zf.write(tmpdir / member, arcname=member)

    return output_path


def _write_occurrence(df: pd.DataFrame, output_path: Path) -> None:
    """Write the Darwin Core ``occurrence.txt`` table.

    :param df: Canonical dataframe filtered to one analysis method.
    :param output_path: Destination occurrence table path.
    """
    occurrence = pd.DataFrame(index=df.index)
    occurrence["occurrenceID"] = df["occurrence_id"]
    occurrence["eventID"] = df["event_id"]
    occurrence["eventDate"] = df["collection_date"]
    occurrence["country"] = df["country"]
    occurrence["decimalLatitude"] = df["latitude"]
    occurrence["decimalLongitude"] = df["longitude"]
    occurrence["verbatimDepth"] = df["depth"]
    occurrence["institutionCode"] = df["institution_code"]
    occurrence["basisOfRecord"] = "MaterialSample"
    occurrence["scientificName"] = df["scientific_name"]
    occurrence["taxonID"] = df["taxon_id"]
    occurrence["kingdom"] = df["kingdom"]
    occurrence["phylum"] = df["phylum"].fillna(df["division"])
    occurrence["class"] = df["class"]
    occurrence["order"] = df["order"]
    occurrence["family"] = df["family"]
    occurrence["genus"] = df["genus"]
    occurrence["specificEpithet"] = df["species"]
    occurrence["organismQuantity"] = df["measurement_value"]
    occurrence["organismQuantityType"] = df["measurement_unit"]
    occurrence["identifiedBy"] = df["asv_caller"].fillna("MAPseq")
    occurrence["identificationReferences"] = df["reference_database"]
    occurrence = occurrence[OCCURRENCE_COLUMNS]
    occurrence.to_csv(
        output_path,
        sep="\t",
        index=False,
        na_rep="",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )


def _write_meta_xml(output_path: Path) -> None:
    """Write Darwin Core Archive metadata describing the occurrence core.

    :param output_path: Destination ``meta.xml`` path.
    """
    ElementTree.register_namespace("", DWCA_TEXT_NAMESPACE)
    archive = ElementTree.Element(
        f"{{{DWCA_TEXT_NAMESPACE}}}archive",
        {"metadata": "eml.xml"},
    )
    core = ElementTree.SubElement(
        archive,
        f"{{{DWCA_TEXT_NAMESPACE}}}core",
        {
            "encoding": "UTF-8",
            "fieldsTerminatedBy": "\\t",
            "linesTerminatedBy": "\\n",
            "ignoreHeaderLines": "1",
            "rowType": f"{DWCA_TERM_NAMESPACE}Occurrence",
        },
    )
    files = ElementTree.SubElement(core, f"{{{DWCA_TEXT_NAMESPACE}}}files")
    location = ElementTree.SubElement(files, f"{{{DWCA_TEXT_NAMESPACE}}}location")
    location.text = "occurrence.txt"
    ElementTree.SubElement(core, f"{{{DWCA_TEXT_NAMESPACE}}}id", {"index": "0"})

    for index, column in enumerate(OCCURRENCE_COLUMNS):
        ElementTree.SubElement(
            core,
            f"{{{DWCA_TEXT_NAMESPACE}}}field",
            {
                "index": str(index),
                "term": f"{DWCA_TERM_NAMESPACE}{column}",
            },
        )

    tree = ElementTree.ElementTree(archive)
    ElementTree.indent(tree, space="  ")
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def _write_eml_xml(
    study: Study,
    df: pd.DataFrame,
    analysis_method: str,
    reference_database: str | None,
    amplified_region: str | None,
    output_path: Path,
) -> None:
    """Write EML metadata for a Darwin Core Archive.

    :param study: Study used for title, accession, and metadata fields.
    :param df: Canonical dataframe filtered to one archive product.
    :param analysis_method: Analysis method represented by the archive.
    :param reference_database: Reference database represented by the archive.
    :param amplified_region: Amplified region represented by the archive, for ASV
        products.
    :param output_path: Destination ``eml.xml`` path.
    """
    method_label = "ASV" if analysis_method == "asv" else "closed-reference"
    product_label = _product_label(method_label, reference_database, amplified_region)
    pipeline_versions = sorted(
        str(value) for value in df["pipeline_version"].dropna().unique()
    )
    pipeline_version = ", ".join(pipeline_versions) or "unknown"
    study_description = (
        study.metadata.get(ENAStudyFields.STUDY_DESCRIPTION) or study.title
    )
    centre_name = study.metadata.get(ENAStudyFields.CENTER_NAME) or "MGnify"

    dataset = Dataset(
        title=f"{study.accession} amplicon {pipeline_version} {product_label} taxonomic occurrences",
        creator=[Creator(organizationName=centre_name)],
        metadataProvider=[MetadataProvider(organizationName="MGnify")],
        contact=[Contact(organizationName=centre_name)],
        intellectualRights=IntellectualRights(
            para=EMG_CONFIG.darwin_core_archive.license_text,
            license=License(
                url=EMG_CONFIG.darwin_core_archive.license_url,
                value=EMG_CONFIG.darwin_core_archive.license_name,
            ),
        ),
        abstract=Abstract(
            para=[
                f"{method_label} taxonomic occurrence archive for MGnify study {study.accession}.",
                f"Product: {product_label}.",
                study_description,
            ]
        ),
        keywordSet=KeywordSet(keyword=EMG_CONFIG.darwin_core_archive.keywords),
        language=EMG_CONFIG.darwin_core_archive.language,
        methods=Methods(
            methodStep=[
                MethodStep(
                    description=Abstract(
                        para=[
                            f"Generated from MGnify amplicon pipeline {pipeline_version} {product_label} taxonomy outputs."
                        ]
                    )
                )
            ],
            sampling=Sampling(samplingDescription=Abstract(para=[study_description])),
        ),
        pubDate=datetime.now(),
        distribution=Distribution(
            online=DistributionOnline(
                url=urljoin(
                    EMG_CONFIG.darwin_core_archive.studies_url_root_for_distribution,
                    study.accession,
                )
            )
        ),
    )

    eml = Eml(
        scope="GBIF-IPT",
        dataset=dataset,
        xsi_schemaLocation=GBIF_GMP_XSD,
        packageId=f"{study.accession}/amplicon/{pipeline_version}/{analysis_method}/{reference_database or 'all'}/{amplified_region or 'all'}",
    )
    content = eml.to_xml(encoding="utf-8", xml_declaration=True, pretty_print=True)
    output_path.write_text(content.decode("utf-8"), encoding="utf-8")


def _product_label(
    method_label: str,
    reference_database: str | None,
    amplified_region: str | None,
) -> str:
    """Build a human-readable archive product label.

    :param method_label: Display label for the analysis method.
    :param reference_database: Reference database represented by the product.
    :param amplified_region: Amplified region represented by the product.
    :return: Human-readable label for EML title/description fields.
    """
    parts = [method_label]
    if reference_database:
        parts.append(reference_database)
    if amplified_region:
        parts.append(amplified_region)
    return " ".join(parts)
