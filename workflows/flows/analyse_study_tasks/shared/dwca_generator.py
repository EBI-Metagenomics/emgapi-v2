import pathlib
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
from zipfile import ZIP_DEFLATED, ZipFile
import xml.etree.ElementTree as ET

import pandas as pd
from prefect import get_run_logger
from pydantic import BaseModel, ConfigDict, Field

from activate_django_first import EMG_CONFIG
from analyses.models import Analysis, Study
from workflows.data_io_utils.darwin_core.dwca_models import (
    Abstract,
    IntellectualRights,
    License,
    Dataset,
    Eml,
    GBIF_GMP_XSD,
    Methods,
    MethodStep,
    Sampling,
    KeywordSet,
    Distribution,
    DistributionOnline,
    Creator,
    Contact,
    MetadataProvider,
)
from workflows.ena_utils.study import ENAStudyFields
from workflows.prefect_utils.flows_utils import django_db_task as task


DWCA_TEXT_NS = "http://rs.tdwg.org/dwc/text/"
DWC_TERMS_NS = "http://rs.tdwg.org/dwc/terms/"


class DwcaEventRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventID")
    parent_event_id: str | None = Field(default=None, alias="parentEventID")
    dataset_id: str | None = Field(default=None, alias="datasetID")
    material_sample_id: str | None = Field(default=None, alias="materialSampleID")
    sampling_protocol: str | None = Field(default=None, alias="samplingProtocol")
    sample_size_value: float | None = Field(default=None, alias="sampleSizeValue")
    sample_size_unit: str | None = Field(default=None, alias="sampleSizeUnit")
    event_date: str | None = Field(default=None, alias="eventDate")
    country: str | None = None
    decimal_latitude: float | None = Field(default=None, alias="decimalLatitude")
    decimal_longitude: float | None = Field(default=None, alias="decimalLongitude")
    verbatim_depth: str | None = Field(default=None, alias="verbatimDepth")
    institution_code: str | None = Field(default=None, alias="institutionCode")


class DwcaOccurrenceRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventID")
    occurrence_id: str = Field(alias="occurrenceID")
    basis_of_record: str = Field(alias="basisOfRecord", default="MaterialSample")
    occurrence_status: str = Field(alias="occurrenceStatus", default="present")
    organism_quantity: float | None = Field(default=None, alias="organismQuantity")
    organism_quantity_type: str | None = Field(
        default=None, alias="organismQuantityType"
    )
    associated_sequences: str | None = Field(default=None, alias="associatedSequences")
    taxon_id: str | None = Field(default=None, alias="taxonID")
    scientific_name: str | None = Field(default=None, alias="scientificName")
    kingdom: str | None = None
    phylum: str | None = None
    class_: str | None = Field(default=None, alias="class")
    order: str | None = None
    family: str | None = None
    genus: str | None = None
    specific_epithet: str | None = Field(default=None, alias="specificEpithet")
    identification_references: str | None = Field(
        default=None, alias="identificationReferences"
    )
    identification_qualifier: float | None = Field(
        default=None, alias="identificationQualifier"
    )


@task()
def convert_dwcr_to_dwca(
    study_accession: str,
    experiment_type: Analysis.ExperimentTypes,
    pipeline: Analysis.PipelineVersions,
    # dwcr_file: pathlib.Path,
    # pipeline_version: Analysis.PipelineVersions,
    out_path: pathlib.Path | None = None,
):
    logger = get_run_logger()
    study = Study.objects.get(accession=study_accession)
    logger.info(f"Generating Darwin Core Archive for {study}")

    study_description = (
        study.metadata.get(ENAStudyFields.STUDY_DESCRIPTION) or study.title
    )
    abstract = Abstract(para=[study_description])

    creator = Creator(
        organizationName=study.metadata.get(ENAStudyFields.CENTER_NAME),
    )

    contact = Contact(
        organizationName=study.metadata.get(ENAStudyFields.CENTER_NAME),
    )

    metadata_provider = MetadataProvider(
        organizationName=study.metadata.get(ENAStudyFields.CENTER_NAME),
    )

    license = IntellectualRights(
        para=EMG_CONFIG.darwin_core_archive.license_text,
        license=License(
            url=EMG_CONFIG.darwin_core_archive.license_url,
            value=EMG_CONFIG.darwin_core_archive.license_name,
        ),
    )

    methods = Methods(
        methodStep=[
            MethodStep(
                description=Abstract(
                    para=[f"Pipeline used: MGnify {experiment_type} {pipeline.value}"]
                )
            )
        ],
        sampling=Sampling(samplingDescription=Abstract(para=[study_description])),
    )

    dataset = Dataset(
        title=study.title,
        creator=[creator],
        metadataProvider=[metadata_provider],
        contact=[contact],
        intellectualRights=license,
        abstract=abstract,
        keywordSet=KeywordSet(keyword=EMG_CONFIG.darwin_core_archive.keywords),
        language=EMG_CONFIG.darwin_core_archive.language,
        methods=methods,
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
        xsi_schemaLocation=f"eml://ecoinformatics.org/eml-2.1.1 {GBIF_GMP_XSD}",
        packageId=f"{study_accession}/{experiment_type.value}/{datetime.now().isoformat()}.xml",
    )

    dwca_dir = study.results_dir_path / "dwca"
    dwca_dir.mkdir(parents=True, exist_ok=True)

    eml_path = dwca_dir / "eml.xml"
    content = eml.to_xml(encoding="utf-8", xml_declaration=True, pretty_print=True)
    eml_path.write_text(content.decode(encoding="utf-8"), encoding="utf-8")

    dwcr_files = _find_merged_dwcr_files(study.results_dir_path)
    if not dwcr_files:
        raise FileNotFoundError(
            f"No DwC-R summary files found for {study} in {study.results_dir_path}"
        )

    dwcr_df = _read_dwcr_files(dwcr_files)
    event_df = _build_event_df(dwcr_df)
    occurrence_df = _build_occurrence_df(dwcr_df)

    event_path = dwca_dir / "event.txt"
    occurrence_path = dwca_dir / "occurrence.txt"
    event_df.to_csv(event_path, sep="\t", index=False, na_rep="")
    occurrence_df.to_csv(occurrence_path, sep="\t", index=False, na_rep="")

    meta_path = dwca_dir / "meta.xml"
    _write_meta_xml(meta_path, event_df.columns, occurrence_df.columns)

    archive_path = dwca_dir / f"{study_accession}_dwca.zip"
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in (eml_path, meta_path, event_path, occurrence_path):
            archive.write(path, arcname=path.name)

    logger.info(f"Generated Darwin Core Archive for {study} at {archive_path}")

    if out_path:
        archive_path = shutil.move(archive_path, out_path)

    return archive_path


@task()
def add_dwca__to_downloads(mgnify_study_accession: str):
    # TODO: gotta do this once the dwca generator is ready
    pass


def _read_dwcr_files(dwcr_files: list[Path]) -> pd.DataFrame:
    frames = []
    for dwcr_file in dwcr_files:
        frame = pd.read_csv(
            dwcr_file,
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA"],
        )
        frame["sourceDwcrFile"] = dwcr_file.name
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _find_merged_dwcr_files(results_dir: Path) -> list[Path]:
    return sorted(
        list(results_dir.glob("*_dwcready.csv"))
        + list(results_dir.glob("*/*_dwcready.csv"))
        + list((results_dir / "study-summaries").glob("*_dwcready.csv"))
    )


def _build_event_df(dwcr_df: pd.DataFrame) -> pd.DataFrame:
    event_columns = [
        "StudyID",
        "SampleID",
        "RunID",
        "seq_meth",
        "collectionDate",
        "country",
        "decimalLatitude",
        "decimalLongitude",
        "depth",
        "InstitutionCode",
    ]
    events = dwcr_df.loc[:, event_columns].drop_duplicates(subset=["RunID"]).copy()
    per_run_readcount = (
        pd.to_numeric(dwcr_df["MeasurementValue"], errors="coerce")
        .groupby(dwcr_df["RunID"])
        .sum(min_count=1)
        .rename("sampleSizeValue")
    )
    events = events.join(per_run_readcount, on="RunID")

    event_df = pd.DataFrame(
        {
            "eventID": events["RunID"],
            "parentEventID": events["StudyID"],
            "datasetID": events["StudyID"],
            "materialSampleID": events["SampleID"],
            "samplingProtocol": events["seq_meth"],
            "sampleSizeValue": events["sampleSizeValue"],
            "sampleSizeUnit": "DNA sequence reads",
            "eventDate": events["collectionDate"],
            "country": events["country"],
            "decimalLatitude": pd.to_numeric(
                events["decimalLatitude"], errors="coerce"
            ),
            "decimalLongitude": pd.to_numeric(
                events["decimalLongitude"], errors="coerce"
            ),
            "verbatimDepth": events["depth"],
            "institutionCode": events["InstitutionCode"],
        }
    )
    return _validated_dataframe(event_df, DwcaEventRecord)


def _build_occurrence_df(dwcr_df: pd.DataFrame) -> pd.DataFrame:
    occurrence_df = pd.DataFrame(
        {
            "eventID": dwcr_df["RunID"],
            "occurrenceID": dwcr_df.apply(_occurrence_id, axis=1),
            "basisOfRecord": "MaterialSample",
            "occurrenceStatus": "present",
            "organismQuantity": pd.to_numeric(
                dwcr_df["MeasurementValue"], errors="coerce"
            ),
            "organismQuantityType": dwcr_df["MeasurementUnit"],
            "associatedSequences": dwcr_df["ASVSeq"],
            "taxonID": dwcr_df["taxonID"],
            "scientificName": dwcr_df.apply(_scientific_name, axis=1),
            "kingdom": dwcr_df["Kingdom"].fillna(dwcr_df["Superkingdom"]),
            "phylum": dwcr_df["Phylum"],
            "class": dwcr_df["Class"],
            "order": dwcr_df["Order"],
            "family": dwcr_df["Family"],
            "genus": dwcr_df["Genus"],
            "specificEpithet": dwcr_df["Species"],
            "identificationReferences": dwcr_df["dbhit"],
            "identificationQualifier": pd.to_numeric(
                dwcr_df["dbhitIdentity"], errors="coerce"
            ),
        }
    )
    return _validated_dataframe(occurrence_df, DwcaOccurrenceRecord)


def _occurrence_id(row: pd.Series) -> str:
    return f"{row['RunID']}:{row['sourceDwcrFile']}:{row['ASVID']}"


def _scientific_name(row: pd.Series) -> str | None:
    for column in (
        "Species",
        "Genus",
        "Family",
        "Order",
        "Class",
        "Phylum",
        "Kingdom",
        "Superkingdom",
    ):
        value = row.get(column)
        if pd.notna(value) and value:
            return str(value).replace("_", " ")
    return None


def _validated_dataframe(
    dataframe: pd.DataFrame, record_model: type[BaseModel]
) -> pd.DataFrame:
    records = [
        record_model(**_none_for_missing(record)).model_dump(by_alias=True)
        for record in dataframe.to_dict(orient="records")
    ]
    return pd.DataFrame(records, columns=list(dataframe.columns))


def _none_for_missing(record: dict) -> dict:
    return {key: None if pd.isna(value) else value for key, value in record.items()}


def _write_meta_xml(
    meta_path: Path,
    event_columns: pd.Index,
    occurrence_columns: pd.Index,
) -> None:
    ET.register_namespace("", DWCA_TEXT_NS)
    ET.register_namespace("dwc", DWC_TERMS_NS)
    archive = ET.Element(
        _dwca_xml_tag("archive"),
        {
            "metadata": "eml.xml",
        },
    )
    core = _dwca_file_element(archive, "core", "event.txt", "Event")
    ET.SubElement(core, _dwca_xml_tag("id"), {"index": "0"})
    _add_fields(core, event_columns)

    extension = _dwca_file_element(archive, "extension", "occurrence.txt", "Occurrence")
    ET.SubElement(extension, _dwca_xml_tag("coreid"), {"index": "0"})
    _add_fields(extension, occurrence_columns[1:], start_index=1)

    tree = ET.ElementTree(archive)
    ET.indent(tree, space="  ")
    tree.write(meta_path, encoding="utf-8", xml_declaration=True)


def _dwca_file_element(
    archive: ET.Element, tag: str, location: str, row_type: str
) -> ET.Element:
    element = ET.SubElement(
        archive,
        _dwca_xml_tag(tag),
        {
            "encoding": "UTF-8",
            "fieldsTerminatedBy": "\t",
            "linesTerminatedBy": "\n",
            "fieldsEnclosedBy": "",
            "ignoreHeaderLines": "1",
            "rowType": f"{DWC_TERMS_NS}{row_type}",
        },
    )
    files = ET.SubElement(element, _dwca_xml_tag("files"))
    ET.SubElement(files, _dwca_xml_tag("location")).text = location
    return element


def _add_fields(element: ET.Element, columns: pd.Index, start_index: int = 0) -> None:
    for index, column in enumerate(columns, start=start_index):
        ET.SubElement(
            element,
            _dwca_xml_tag("field"),
            {"index": str(index), "term": f"{DWC_TERMS_NS}{column}"},
        )


def _dwca_xml_tag(tag: str) -> str:
    return f"{{{DWCA_TEXT_NS}}}{tag}"
