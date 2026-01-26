import pathlib
from datetime import datetime
from urllib.parse import urljoin

from prefect import task, get_run_logger

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


# from dwca import DarwinCoreArchive
# from eml.types import ResponsibleParty, IndividualName


@task
def convert_dwcr_to_dwca(
    study_accession: str,
    experiment_type: Analysis.ExperimentTypes,
    pipeline: Analysis.PipelineVersions,
    # dwcr_file: pathlib.Path,
    # pipeline_version: Analysis.PipelineVersions,
    # out_path: pathlib.Path,
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
        xsi_schemaLocation=GBIF_GMP_XSD,
        packageId=f"{study_accession}/{experiment_type.value}/{datetime.now()}.xml",
    )

    # TODO: the namespace and source locations are not quite right. check GBIF standards.
    # should be something like
    # <eml:eml
    #     xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"
    #     xmlns:dc="http://purl.org/dc/terms/"
    #     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    #     xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1
    #                         http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd"
    #     packageId="UUID-or-identifier"
    #     system="http://gbif.org"
    #     xml:lang="en">

    dwca_dir = pathlib.Path(study.results_dir) / "dwca"
    dwca_dir.mkdir(exist_ok=True)

    with dwca_dir / "eml.xml" as eml_file:
        content = eml.to_xml(encoding="utf-8", xml_declaration=False, pretty_print=True)
        eml_file.write_text(content.decode(encoding="utf-8"), encoding="utf-8")

    logger.info(f"Generated Darwin Core Archive for {study} at {dwca_dir}")
    return dwca_dir

    # output events.txt
    # TODO: add sequencing protocol + country
    # TODO: change depth to verbatimDepth in toolkit
    # events_txt_columns = [
    #     "StudyID",
    #     "SampleID",
    #     "RunID",
    #     "decimalLongitude",
    #     "decimalLatitude",
    #     "depth",
    #     "temperature",
    #     "salinity",
    #     "collectionDate",
    #     "InstitutionCode",
    # ]
    #
    # dwcr_df = pd.read_csv(dwcr_file)
    # events_df = dwcr_df.loc[:, events_txt_columns]
    # events_df = events_df.drop_duplicates()
    #
    # per_run_readcount = (
    #     dwcr_df.groupby("RunID")["MeasurementValue"]
    #     .sum()
    #     .reset_index()
    #     .set_index("RunID")
    # )
    # events_df = events_df.join(per_run_readcount, on="RunID")
    # events_df["sampleSizeUnit"] = ["DNA sequence reads"] * len(events_df)
    # events_df = events_df.rename(
    #     {"collectionDate": "eventDate", "MeasurementValue": "sampleSizeValue"}
    # )
    #
    # with tempfile.TemporaryDirectory() as tmpdirname:
    #     events_df.to_csv(f"{tmpdirname}/events.txt")
    #
    #     # TODO: with pydantic :)
    #
    #     # darwin_core = DarwinCoreArchive("testing")
    #     # darwin_core.generate_eml()
    #     # eml = darwin_core.metadata
    #     # eml.initialize_resource(
    #     #     "Example for Darwin Core Archive",
    #     #     ResponsibleParty(
    #     #         individual_name=IndividualName(
    #     #             id="1", last_name="Doe", first_name="John", salutation="Mr."
    #     #         )
    #     #     ),
    #     #     contact=[ResponsibleParty(_id="1", referencing=True)],
    #     # )
    #
    #     # darwin_core.set_core(f"{tmpdirname}/events.txt")
    #     #
    #     # with open("example.zip", "wb") as example_file:
    #     #     darwin_core.to_file(example_file)
    #
    # # logger.info(events_df)
    #
    # # total_reads = dwcr_df["MeasurementValue"]
    #
    # logger.info(events_df)

    # take dwcr
    # grab MGYA for every run
    # output eml.xml using pydwca
    # output meta.xml
    # output occurrences.txt

    # archive into zip using pydwca
