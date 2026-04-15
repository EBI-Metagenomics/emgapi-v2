import pytest
from django.core.management import call_command
from analyses.models import Study, Analysis
from analyses.base_models.with_experiment_type_models import WithExperimentTypeModel
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)


@pytest.mark.django_db
def test_add_pipeline_to_study_downloads_download_groups(
    raw_reads_mgnify_study, raw_reads_mgnify_sample, raw_read_run
):
    # Setup: 3 studies with different types of analyses
    sample = raw_reads_mgnify_sample[0]
    run = raw_read_run[0]

    # 1. Amplicon Study
    amplicon_study = Study.objects.create(
        accession="MGYS00001001",
        title="Amplicon Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    Analysis.objects.create(
        accession="MGYA00001001",
        study=amplicon_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.AMPLICON,
    )
    # Test with both "study_summary.suffix" and just "study_summary"
    amplicon_study.add_download(
        DownloadFile(
            path="s1.tsv",
            alias="a1",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary.silva-ssu",
            short_description="short",
            long_description="long",
        )
    )
    amplicon_study.add_download(
        DownloadFile(
            path="s1_2.tsv",
            alias="a1_2",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary",
            short_description="short",
            long_description="long",
        )
    )

    # 2. Assembly Study
    assembly_study = Study.objects.create(
        accession="MGYS00001002",
        title="Assembly Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    Analysis.objects.create(
        accession="MGYA00001002",
        study=assembly_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.ASSEMBLY,
    )
    assembly_study.add_download(
        DownloadFile(
            path="s2.tsv",
            alias="a2",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary.ko",
            short_description="short",
            long_description="long",
        )
    )

    # 3. Raw Reads Study (Metagenomic)
    rawreads_study = Study.objects.create(
        accession="MGYS00001003",
        title="Raw Reads Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    Analysis.objects.create(
        accession="MGYA00001003",
        study=rawreads_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.METAGENOMIC,
    )
    rawreads_study.add_download(
        DownloadFile(
            path="s3.tsv",
            alias="a3",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary.motus",
            short_description="short",
            long_description="long",
        )
    )

    # 4. Study with mixed raw reads
    mixed_study = Study.objects.create(
        accession="MGYS00001004",
        title="Mixed Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    Analysis.objects.create(
        accession="MGYA00001004",
        study=mixed_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.METAGENOMIC,
    )
    Analysis.objects.create(
        accession="MGYA00001005",
        study=mixed_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.METATRANSCRIPTOMIC,
    )
    mixed_study.add_download(
        DownloadFile(
            path="s4.tsv",
            alias="a4",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary.goslim",
            short_description="short",
            long_description="long",
        )
    )

    # 5. Study with no analyses (should be skipped)
    empty_study = Study.objects.create(
        accession="MGYS00001005",
        title="Empty Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    empty_study.add_download(
        DownloadFile(
            path="s5.tsv",
            alias="a5",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            file_type=DownloadFileType.TSV,
            download_group="study_summary.something",
            short_description="short",
            long_description="long",
        )
    )

    # 6. Study with no downloads (should be excluded from queryset)
    nodl_study = Study.objects.create(
        accession="MGYS00001006",
        title="No Downloads Study",
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    # Give it an analysis so it WOULD be processed if it had downloads
    Analysis.objects.create(
        accession="MGYA00001006",
        study=nodl_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.AMPLICON,
    )

    # Run the command
    call_command("add_pipeline_to_study_downloads_download_groups")

    # Refresh from DB and verify
    amplicon_study.refresh_from_db()
    assert (
        amplicon_study.downloads[0]["download_group"]
        == "study_summary.v6.amplicon.silva-ssu"
    )
    assert amplicon_study.downloads[1]["download_group"] == "study_summary.v6.amplicon"

    assembly_study.refresh_from_db()
    assert (
        assembly_study.downloads[0]["download_group"] == "study_summary.v6.assembly.ko"
    )

    rawreads_study.refresh_from_db()
    assert (
        rawreads_study.downloads[0]["download_group"]
        == "study_summary.v6.rawreads.motus"
    )

    mixed_study.refresh_from_db()
    assert (
        mixed_study.downloads[0]["download_group"] == "study_summary.v6.rawreads.goslim"
    )

    empty_study.refresh_from_db()
    assert empty_study.downloads[0]["download_group"] == "study_summary.something"

    nodl_study.refresh_from_db()
    assert nodl_study.downloads == []
