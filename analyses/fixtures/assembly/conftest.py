import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def mgnify_assemblies(raw_read_run, raw_reads_mgnify_study, assemblers):
    assembler_metaspades = assemblers.filter(
        name=mg_models.Assembler.METASPADES
    ).first()
    assembler_megahit = assemblers.filter(name=mg_models.Assembler.MEGAHIT).first()
    # create assembly objects skipping AMPLICON runs
    assembleable_runs = [
        run
        for run in raw_read_run
        if run.experiment_type != mg_models.Run.ExperimentTypes.AMPLICON.value
    ]
    assembly_objects = []
    # create metaspades assemblies
    for run in assembleable_runs:
        assembly_obj, _ = mg_models.Assembly.objects.get_or_create(
            run=run,
            reads_study=raw_reads_mgnify_study,
            ena_study=raw_reads_mgnify_study.ena_study,
            assembler=assembler_metaspades,
            dir="slurm-dev-environment/fs/hps/tests/assembly_uploader",
            metadata={"coverage": 20},
            sample=run.sample,
            ena_accessions=[
                "ERZ857107",
            ],
        )
        assembly_objects.append(assembly_obj)

    # create one megahit assembly
    for run in assembleable_runs[:1]:
        assembly, _ = mg_models.Assembly.objects.get_or_create(
            run=run,
            reads_study=raw_reads_mgnify_study,
            ena_study=raw_reads_mgnify_study.ena_study,
            assembler=assembler_megahit,
            dir="/hps/tests/assembly_uploader",
            metadata={"coverage": 10},
            sample=run.sample,
            ena_accessions=["ERZ857108"],
        )
        assembly_objects.append(assembly)
    return assembly_objects


@pytest.fixture
def mgnify_assemblies_completed(mgnify_assemblies):
    run_accession = "SRR6180434"
    metaspades_assemblies = mg_models.Assembly.objects.filter(
        assembler__name="metaspades", run__ena_accessions__contains=[run_accession]
    )
    for item in metaspades_assemblies:
        item.mark_status("assembly_started")
        item.mark_status("assembly_completed")
    return metaspades_assemblies


@pytest.fixture
def mgnify_assembly_completed_uploader_sanity_check(mgnify_assemblies):
    run_accession = "SRR6180435"
    metaspades_assemblies = mg_models.Assembly.objects.filter(
        assembler__name="metaspades", run__ena_accessions__contains=[run_accession]
    )
    for item in metaspades_assemblies:
        item.mark_status("assembly_completed")
    return metaspades_assemblies


@pytest.fixture
def assembly_with_analyses(mgnify_assemblies, raw_reads_mgnify_study):
    """
    Create assembly analyses for testing batch operations.
    Returns a list of Analysis objects linked to assemblies.
    """
    analyses = []
    # We only need a subset, that is why 3 (just an arbitrary cut)
    for assembly in mgnify_assemblies[:3]:
        analysis, _ = mg_models.Analysis.objects.get_or_create(
            study=raw_reads_mgnify_study,
            sample=assembly.sample,
            assembly=assembly,
            ena_study=raw_reads_mgnify_study.ena_study,
            pipeline_version=mg_models.Analysis.PipelineVersions.v6,
        )
        analysis.inherit_experiment_type()
        analyses.append(analysis)
    return analyses
