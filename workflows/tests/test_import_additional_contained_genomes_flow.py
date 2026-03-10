import pytest
from prefect import flow

from analyses.models import Assembly, Biome, Run, Sample, Study
from ena.models import Study as ENAStudy, Sample as ENASample
from genomes.models import Genome, GenomeCatalogue, AdditionalContainedGenomes
from workflows.flows.import_additional_contained_genomes_flow import (
    import_additional_contained_genomes_flow,
    validate_csv_file,
)
from workflows.prefect_utils.testing_utils import (
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)


def _make_basic_genome_env():
    """
    Create the minimal set of objects required to create Genomes referencing a catalogue and biome.
    Returns (biome, catalogue)
    """
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )
    return biome, catalogue


def _make_ena_study_sample_run(accession: str):
    """
    Make ENA study/sample, mgnify study/sample, and a Run whose ena_accessions contain `accession`.
    Returns (ena_study, study, ena_sample, sample, run)
    """
    ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="ENA Study")
    study = Study.objects.create(
        accession="MGYS00000001", ena_study=ena_study, title="Study"
    )

    ena_sample = ENASample.objects.create(accession="ERS123456", study=ena_study)
    sample = Sample.objects.create(ena_sample=ena_sample, ena_study=ena_study)
    sample.studies.add(study)

    run = Run.objects.create(study=study, sample=sample, ena_study=ena_study)
    run.ena_accessions = [accession]
    run.save()

    return ena_study, study, ena_sample, sample, run


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_validate_csv_file(prefect_harness, tmp_path):
    # Prepare a real temp CSV file
    csv_path = tmp_path / "test.tsv"
    csv_path.write_text(
        "Run\tGenome_Mgnify_accession\tContainment\tcANI\nSRR1\tMGYG0001\t0.1\t0.9\n"
    )

    @flow(name="Test Validate CSV File")
    def test_flow(file_path: str):
        return validate_csv_file(file_path)

    # Valid file returns input path
    result = run_flow_and_capture_logs(test_flow, str(csv_path)).result
    assert result == str(csv_path)

    # Nonexistent file
    with pytest.raises(FileNotFoundError):
        run_flow_and_capture_logs(test_flow, str(tmp_path / "nope.tsv"))

    # Directory instead of file
    with pytest.raises(ValueError):
        run_flow_and_capture_logs(test_flow, str(tmp_path))


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_import_additional_contained_genomes_flow_success(prefect_harness, tmp_path):
    # Minimal genome/cataloque/biome setup
    biome, catalogue = _make_basic_genome_env()

    # Genome to be linked
    genome = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type="MAG",
        completeness=95.0,
        contamination=2.0,
        trnas=20.0,
        nc_rnas=10,
        num_proteins=1000,
        eggnog_coverage=80.0,
        ipr_coverage=75.0,
        taxon_lineage="Bacteria;Proteobacteria;Gammaproteobacteria",
        catalogue=catalogue,
    )

    # Create Run with accession and two Assemblies: one FK-linked, one by overlap with run accession
    run_acc = "SRR6180434"
    ena_study, study, ena_sample, sample, run = _make_ena_study_sample_run(run_acc)

    assembly_fk = Assembly.objects.create_for_runs_and_sample(
        runs=[run],
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )
    assembly_fk.ena_accessions = ["ERZ123456"]
    assembly_fk.save()

    assembly_overlap = Assembly.objects.create_for_runs_and_sample(
        runs=[run],
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )
    assembly_overlap.save()

    # should be two diff assemblies of same run
    assert assembly_fk.pk != assembly_overlap.pk

    # Prepare TSV with a single valid row
    csv_path = tmp_path / "acg.tsv"
    csv_path.write_text(
        "Run\tGenome_Mgnify_accession\tContainment\tcANI\n"
        f"{run_acc}\t{genome.accession}\t0.65\t0.97\n"
    )

    # Run the flow with small chunk sizes
    logged = run_flow_and_capture_logs(
        import_additional_contained_genomes_flow,
        str(csv_path),
        2,  # chunk_size
        2,  # batch_size
    )
    result = logged.result

    assert result["total_rows"] == 1
    assert result["created_attempts"] == 2
    assert result["skipped_rows"] == 0
    assert result["missing_run_or_genome_rows"] == 0

    rows = list(
        AdditionalContainedGenomes.objects.filter(run=run, genome=genome).order_by(
            "assembly_id"
        )
    )
    assert len(rows) == 2
    assert {r.assembly_id for r in rows} == {assembly_fk.id, assembly_overlap.id}
    assert all(abs(r.containment - 0.65) < 1e-9 for r in rows)
    assert all(abs(r.cani - 0.97) < 1e-9 for r in rows)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_bom_header_is_accepted(prefect_harness, tmp_path):
    # Set up second run/genome to avoid unique conflicts from previous test
    biome, catalogue = _make_basic_genome_env()
    genome = Genome.objects.create(
        accession="MGYG000000002",
        biome=biome,
        length=123,
        num_contigs=1,
        n_50=10,
        gc_content=0.4,
        type="MAG",
        completeness=90.0,
        contamination=1.0,
        trnas=1.0,
        nc_rnas=1,
        num_proteins=10,
        eggnog_coverage=10.0,
        ipr_coverage=10.0,
        taxon_lineage="Bacteria",
        catalogue=catalogue,
    )

    run_acc = "ERR11846481"
    ena_study, study, ena_sample, sample, run = _make_ena_study_sample_run(run_acc)

    a1 = Assembly.objects.create_for_runs_and_sample(
        runs=[run], sample=sample, reads_study=study, ena_study=ena_study
    )
    a1.save()
    a2 = Assembly.objects.create_for_runs_and_sample(
        runs=[run], sample=sample, reads_study=study, ena_study=ena_study
    )
    a2.save()

    # Write TSV with BOM in the header for the first field name
    csv_path = tmp_path / "bom.tsv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("\ufeffRun\tGenome_Mgnify_accession\tContainment\tcANI\n")
        f.write(f"{run_acc}\t{genome.accession}\t1\t1\n")

    logged = run_flow_and_capture_logs(
        import_additional_contained_genomes_flow,
        str(csv_path),
        10,
        10,
    )
    result = logged.result

    # Should process 1 row and attempt to create 2 records
    assert result["total_rows"] == 1
    assert result["created_attempts"] == 2
    assert (
        AdditionalContainedGenomes.objects.filter(run=run, genome=genome).count() == 2
    )


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_invalid_header_raises_value_error(prefect_harness, tmp_path):
    # Missing the required 'Run' column
    bad_csv = tmp_path / "bad.tsv"
    bad_csv.write_text(
        "RUN_ID\tGenome_Mgnify_accession\tContainment\tcANI\nRRR\tMGYG000000999\t0.1\t0.2\n"
    )

    with pytest.raises(ValueError):
        run_flow_and_capture_logs(
            import_additional_contained_genomes_flow, str(bad_csv), 5, 5
        )
