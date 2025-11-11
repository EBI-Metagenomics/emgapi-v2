import pytest

from analyses.base_models.with_downloads_models import DownloadType
from analyses.models import Study
from ena.models import Study as ENAStudy
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)


@pytest.mark.django_db
class TestAddAssemblyStudySummariesToDownloads:
    """Test the add_assembly_study_summaries_to_downloads task."""

    def test_adds_study_summaries_and_returns_count(self, prefect_harness, tmp_path):
        """Test that study summaries are added with the correct count and metadata."""
        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study, title="Test Study", results_dir=str(tmp_path)
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Create taxonomy and functional summary files
        sources = ["taxonomy", "ko", "pfam"]
        for source in sources:
            (
                tmp_path / f"{study.first_accession}_{source}_study_summary.tsv"
            ).write_text("data\n")

        added_count = add_assembly_study_summaries_to_downloads(study.accession)

        # Verify the count and downloads
        assert added_count == 3
        study.refresh_from_db()
        assert len(study.downloads_as_objects) == 3

        # Verify taxonomy has the correct type
        taxonomy_download = next(
            d
            for d in study.downloads_as_objects
            if d.download_group == "study_summary.taxonomy"
        )
        assert taxonomy_download.download_type == DownloadType.TAXONOMIC_ANALYSIS

        # Verify functional has the correct type
        ko_download = next(
            d
            for d in study.downloads_as_objects
            if d.download_group == "study_summary.ko"
        )
        assert ko_download.download_type == DownloadType.FUNCTIONAL_ANALYSIS

    def test_idempotent_clears_and_readds(self, prefect_harness, tmp_path):
        """Test that running twice is idempotent."""
        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study, title="Test Study", results_dir=str(tmp_path)
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        (tmp_path / f"{study.first_accession}_taxonomy_study_summary.tsv").write_text(
            "data\n"
        )

        # Run twice
        add_assembly_study_summaries_to_downloads(study.accession)
        study.refresh_from_db()
        first_count = len(study.downloads)

        add_assembly_study_summaries_to_downloads(study.accession)
        study.refresh_from_db()

        # Should still have the same count (no duplicates)
        assert len(study.downloads) == first_count

    def test_skips_unrecognized_files_and_empty_dirs(self, prefect_harness, tmp_path):
        """Test that invalid files are skipped and empty dirs return 0."""
        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study, title="Test Study", results_dir=str(tmp_path)
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Create valid and invalid files
        (tmp_path / f"{study.first_accession}_taxonomy_study_summary.tsv").write_text(
            "data\n"
        )
        (tmp_path / f"{study.first_accession}_invalid_study_summary.tsv").write_text(
            "data\n"
        )

        added_count = add_assembly_study_summaries_to_downloads(study.accession)
        assert added_count == 1

        # Test an empty directory
        study2 = Study.objects.create(
            ena_study=ENAStudy.objects.create(accession="PRJEB99999", title="Empty"),
            title="Empty",
            results_dir=str(tmp_path / "empty"),
        )
        (tmp_path / "empty").mkdir()
        assert add_assembly_study_summaries_to_downloads(study2.accession) == 0

    def test_first_run_persists_to_db(self, prefect_harness, tmp_path):
        """Test that first run (starting from zero) correctly persists downloads to DB."""
        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study, title="Test Study", results_dir=str(tmp_path)
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Verify starting with no downloads
        assert len(study.downloads) == 0

        # Create summary files
        (tmp_path / f"{study.first_accession}_taxonomy_study_summary.tsv").write_text(
            "data\n"
        )
        (tmp_path / f"{study.first_accession}_ko_study_summary.tsv").write_text(
            "data\n"
        )

        # Run the task
        added_count = add_assembly_study_summaries_to_downloads(study.accession)
        assert added_count == 2

        # Refresh from DB and verify persistence
        study.refresh_from_db()
        assert len(study.downloads) == 2

        # Verify the downloads are actually in the database by fetching fresh instance
        fresh_study = Study.objects.get(accession=study.accession)
        assert len(fresh_study.downloads) == 2
