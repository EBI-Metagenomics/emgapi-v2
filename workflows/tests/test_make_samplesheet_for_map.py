import csv
import pytest

from activate_django_first import EMG_CONFIG
from analyses.models import Analysis
from workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly import (
    make_samplesheet_for_map,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


@pytest.mark.django_db(transaction=True)
class TestMakeSamplesheetForMap:
    """Test the make_samplesheet_for_map task."""

    def test_make_samplesheet_for_map_logic(
        self,
        prefect_harness,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
    ):
        """Test the logic of make_samplesheet_for_map."""

        # Create analyses with assemblies using fixtures
        # Analysis 1: VIRify COMPLETED, all files exist
        # Analysis 2: VIRify FAILED, VIRify GFF MISSING

        analysis_1 = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )
        analysis_1.assembly.ena_accessions = ["ACC1"]
        analysis_1.assembly.save()

        analysis_2 = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[1],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[1],
        )
        analysis_2.assembly.ena_accessions = ["ACC2"]
        analysis_2.assembly.save()

        batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=raw_reads_mgnify_study,
            workspace_dir=tmp_path,
            skip_completed=False,
        )
        batch = batches[0]

        # Set up workspaces
        asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
        virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)

        # Create mandatory files for analysis_1
        accession_1 = analysis_1.assembly.first_accession
        fasta_1 = (
            asa_workspace
            / accession_1
            / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
            / f"{accession_1}_filtered_contigs.fasta.gz"
        )
        fasta_1.parent.mkdir(parents=True, exist_ok=True)
        fasta_1.touch()
        gff_1 = (
            asa_workspace
            / accession_1
            / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
            / f"{accession_1}_predicted_cds.gff.gz"
        )
        gff_1.parent.mkdir(parents=True, exist_ok=True)
        gff_1.touch()

        # Create optional VIRify GFF for analysis_1
        virify_gff_1 = (
            virify_workspace
            / accession_1
            / EMG_CONFIG.virify_pipeline.final_gff_folder
            / f"{accession_1}_virify.gff.gz"
        )
        virify_gff_1.parent.mkdir(parents=True, exist_ok=True)
        virify_gff_1.touch()

        # Create mandatory files for analysis_2
        accession_2 = analysis_2.assembly.first_accession
        fasta_2 = (
            asa_workspace
            / accession_2
            / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
            / f"{accession_2}_filtered_contigs.fasta.gz"
        )
        fasta_2.parent.mkdir(parents=True, exist_ok=True)
        fasta_2.touch()
        gff_2 = (
            asa_workspace
            / accession_2
            / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
            / f"{accession_2}_predicted_cds.gff.gz"
        )
        gff_2.parent.mkdir(parents=True, exist_ok=True)
        gff_2.touch()

        # Ensure VIRify GFF for analysis_2 DOES NOT EXIST
        virify_gff_2 = (
            virify_workspace
            / accession_2
            / EMG_CONFIG.virify_pipeline.final_gff_folder
            / f"{accession_2}_virify.gff.gz"
        )
        if virify_gff_2.exists():
            virify_gff_2.unlink()

        # Update statuses
        batch.batch_analyses.filter(analysis=analysis_1).update(
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
            map_status=AssemblyAnalysisPipelineStatus.RUNNING,
        )
        batch.batch_analyses.filter(analysis=analysis_2).update(
            virify_status=AssemblyAnalysisPipelineStatus.FAILED,
            map_status=AssemblyAnalysisPipelineStatus.RUNNING,
        )

        # Call the task
        analysis_batch_job_ids = [
            batch.batch_analyses.get(analysis=a).id for a in [analysis_1, analysis_2]
        ]
        samplesheet_path, ss_hash = make_samplesheet_for_map(
            assembly_analysis_batch_id=batch.id,
            analysis_batch_job_ids=analysis_batch_job_ids,
            output_dir=tmp_path / "samplesheets",
        )

        assert samplesheet_path.exists()

        # Verify samplesheet content
        with open(samplesheet_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

        assert len(rows) == 2

        row1 = next(r for r in rows if r["sample"] == accession_1)
        assert row1["assembly"] == str(fasta_1)
        assert row1["user_proteins_gff"] == str(gff_1)
        assert row1["virify_gff"] == str(virify_gff_1)

        row2 = next(r for r in rows if r["sample"] == accession_2)
        assert row2["assembly"] == str(fasta_2)
        assert row2["user_proteins_gff"] == str(gff_2)
        assert row2["virify_gff"] == ""  # Should be empty for FAILED virify

    def test_make_samplesheet_for_map_missing_mandatory(
        self,
        prefect_harness,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
    ):
        """Test that make_samplesheet_for_map raises ValueError if mandatory files are missing."""
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )
        batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=raw_reads_mgnify_study,
            workspace_dir=tmp_path,
            skip_completed=False,
        )
        batch = batches[0]
        batch.batch_analyses.filter(analysis=analysis).update(
            map_status=AssemblyAnalysisPipelineStatus.RUNNING
        )

        # Mandatory files NOT created
        batch_analysis = batch.batch_analyses.get(analysis=analysis)

        with pytest.raises(ValueError, match="Mandatory ASA files missing"):
            make_samplesheet_for_map(
                assembly_analysis_batch_id=batch.id,
                analysis_batch_job_ids=[batch_analysis.id],
                output_dir=tmp_path / "samplesheets",
            )
