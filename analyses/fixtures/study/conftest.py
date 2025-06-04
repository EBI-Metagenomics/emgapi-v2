import django
import pytest

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path

django.setup()

import analyses.models as mg_models


@pytest.fixture
def raw_reads_mgnify_study(raw_read_ena_study, top_level_biomes, admin_user):
    study = mg_models.Study.objects.get_or_create(
        ena_study=raw_read_ena_study, title=raw_read_ena_study.title
    )[0]
    study.inherit_accessions_from_related_ena_object("ena_study")
    study.biome = mg_models.Biome.objects.first()
    study.save()
    return study


@pytest.fixture
def study_downloads(raw_reads_mgnify_study):
    raw_reads_mgnify_study.add_download(
        DownloadFile(
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            alias=f"{raw_reads_mgnify_study.first_accession}_SILVA-SSU_study_summary.tsv",
            short_description="Summary of SILVA-SSU taxonomies",
            long_description="Summary of SILVA-SSU taxonomic assignments, across all runs in the study",
            path="study-summaries/amplicon_study_summary.tsv",
            download_group="study_summary.v6.amplicon",
        )
    )
    raw_reads_mgnify_study.results_dir = "/app/data/tests/amplicon_v6_output"
    raw_reads_mgnify_study.external_results_dir = f"{accession_prefix_separated_dir_path(raw_reads_mgnify_study.first_accession, -3)}/"
    raw_reads_mgnify_study.save()


@pytest.fixture
def webin_private_study(webin_private_ena_study):
    # Create an ENA Study with a webin_submitter
    mgnify_study = mg_models.Study.objects.create(
        accession="MGYS00000999",
        ena_study=webin_private_ena_study,
        title="Private MGnify Study",
        is_private=True,
        webin_submitter=webin_private_ena_study.webin_submitter,
    )
    return mgnify_study
