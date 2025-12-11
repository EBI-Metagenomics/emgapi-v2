import pathlib
import tempfile

from analyses.models import Analysis

# from dwca import DarwinCoreArchive
# from eml.types import ResponsibleParty, IndividualName

import pandas as pd
from prefect import task, get_run_logger


@task
def convert_dwcr_to_dwca(
    dwcr_file: pathlib.Path,
    pipeline_version: Analysis.PipelineVersions,
    out_path: pathlib.Path,
):

    logger = get_run_logger()

    # output events.txt
    # TODO: add sequencing protocol + country
    # TODO: change depth to verbatimDepth in toolkit
    events_txt_columns = [
        "StudyID",
        "SampleID",
        "RunID",
        "decimalLongitude",
        "decimalLatitude",
        "depth",
        "temperature",
        "salinity",
        "collectionDate",
        "InstitutionCode",
    ]

    dwcr_df = pd.read_csv(dwcr_file)
    events_df = dwcr_df.loc[:, events_txt_columns]
    events_df = events_df.drop_duplicates()

    per_run_readcount = (
        dwcr_df.groupby("RunID")["MeasurementValue"]
        .sum()
        .reset_index()
        .set_index("RunID")
    )
    events_df = events_df.join(per_run_readcount, on="RunID")
    events_df["sampleSizeUnit"] = ["DNA sequence reads"] * len(events_df)
    events_df = events_df.rename(
        {"collectionDate": "eventDate", "MeasurementValue": "sampleSizeValue"}
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        events_df.to_csv(f"{tmpdirname}/events.txt")

        # TODO: with pydantic :)

        # darwin_core = DarwinCoreArchive("testing")
        # darwin_core.generate_eml()
        # eml = darwin_core.metadata
        # eml.initialize_resource(
        #     "Example for Darwin Core Archive",
        #     ResponsibleParty(
        #         individual_name=IndividualName(
        #             id="1", last_name="Doe", first_name="John", salutation="Mr."
        #         )
        #     ),
        #     contact=[ResponsibleParty(_id="1", referencing=True)],
        # )

        # darwin_core.set_core(f"{tmpdirname}/events.txt")
        #
        # with open("example.zip", "wb") as example_file:
        #     darwin_core.to_file(example_file)

    # logger.info(events_df)

    # total_reads = dwcr_df["MeasurementValue"]

    logger.info(events_df)

    # take dwcr
    # grab MGYA for every run
    # output eml.xml using pydwca
    # output meta.xml
    # output occurrences.txt

    # archive into zip using pydwca
