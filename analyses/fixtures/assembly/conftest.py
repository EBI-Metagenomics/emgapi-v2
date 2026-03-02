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
    for i, run in enumerate(assembleable_runs):
        assembly_obj, created = (
            mg_models.Assembly.objects.get_or_create_for_run_and_sample(
                run=run,
                sample=run.sample,
                reads_study=raw_reads_mgnify_study,
                ena_study=raw_reads_mgnify_study.ena_study,
                assembler=assembler_metaspades,
                dir="slurm-dev-environment/fs/hps/tests/assembly_uploader",
                metadata={"coverage": 20},
                ena_accessions=[
                    f"ERZ_METASPADES_{i}",
                ],
            )
        )
        assembly_objects.append(assembly_obj)

    # create one megahit assembly
    for i, run in enumerate(assembleable_runs[:1]):
        assembly, created = mg_models.Assembly.objects.get_or_create_for_run_and_sample(
            run=run,
            sample=run.sample,
            reads_study=raw_reads_mgnify_study,
            ena_study=raw_reads_mgnify_study.ena_study,
            assembler=assembler_megahit,
            dir="/hps/tests/assembly_uploader",
            metadata={"coverage": 10},
            ena_accessions=[f"ERZ_MEGAHIT_{i}"],
        )
        assembly_objects.append(assembly)
    return assembly_objects


@pytest.fixture
def mgnify_assemblies_completed(mgnify_assemblies):
    run_accession = "SRR6180434"
    metaspades_assemblies = mg_models.Assembly.objects.filter(
        assembler__name="metaspades", runs__ena_accessions__contains=[run_accession]
    )
    for item in metaspades_assemblies:
        item.mark_status("assembly_started")
        item.mark_status("assembly_completed")
    return metaspades_assemblies


@pytest.fixture
def mgnify_assembly_completed_uploader_sanity_check(mgnify_assemblies):
    run_accession = "SRR6180435"
    metaspades_assemblies = mg_models.Assembly.objects.filter(
        assembler__name="metaspades", runs__ena_accessions__contains=[run_accession]
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


@pytest.fixture
def mgnify_assemblies_with_ena(mgnify_assemblies):
    for i, assembly in enumerate(mgnify_assemblies):
        assembly.ena_accessions = [f"ERZ{i+1}"]
        assembly.save()
    return mgnify_assemblies
