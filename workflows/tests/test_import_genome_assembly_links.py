"""
Tests for the import_genome_assembly_links flow.
"""

import os
import tempfile
from unittest.mock import patch

import pytest
from django.core.exceptions import ObjectDoesNotExist
from prefect import flow

from analyses.models import Assembly, Biome, Run, Sample, Study
from ena.models import Study as ENAStudy, Sample as ENASample
from genomes.models import Genome, GenomeAssemblyLink, GenomeCatalogue
from workflows.flows.import_genome_assembly_links import (
    import_genome_assembly_links,
    validate_tsv_file,
    read_tsv_file,
    process_tsv_records,
    find_objects,
    create_links,
)
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.fixture
def mock_tsv_file():
    """
    Create a temporary TSV file with test data.
    """
    temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".tsv")

    # Write header
    temp_file.write("primary_assembly\tgenome\tmag_accession\tspecies_rep\n")

    # Write test data
    temp_file.write("ERZ123456\tMGYG000000001\tMAGS12345\tGCA_123456789.1\n")
    temp_file.write("ERZ789012\tMGYG000000002\tMAGS67890\tGCA_987654321.1\n")
    temp_file.write("ERZ111111\tMGYG000000003\t\t\n")  # Missing optional fields
    temp_file.write(
        "\tMGYG000000004\tMAGS11111\tGCA_111111111.1\n"
    )  # Missing required field
    temp_file.write(
        "ERZ222222\t\tMAGS22222\tGCA_222222222.1\n"
    )  # Missing required field

    temp_file.close()

    yield temp_file.name

    # Clean up
    os.unlink(temp_file.name)


@pytest.mark.django_db
def test_validate_tsv_file(mock_tsv_file):
    """Test the validate_tsv_file task."""

    # Create a test flow that calls the validate_tsv_file task
    @flow(name="Test Validate TSV File")
    def test_flow(file_path):
        return validate_tsv_file(file_path)

    # Test with valid file
    result = run_flow_and_capture_logs(test_flow, mock_tsv_file).result
    assert result == mock_tsv_file

    # Test with non-existent file
    with pytest.raises(FileNotFoundError):
        run_flow_and_capture_logs(test_flow, "/path/to/nonexistent/file.tsv")

    # Test with directory instead of file
    temp_dir = tempfile.mkdtemp()
    try:
        with pytest.raises(ValueError):
            run_flow_and_capture_logs(test_flow, temp_dir)
    finally:
        os.rmdir(temp_dir)


@pytest.mark.django_db
def test_read_tsv_file(mock_tsv_file):
    """Test the read_tsv_file task."""

    # Create a test flow that calls the read_tsv_file task
    @flow(name="Test Read TSV File")
    def test_flow(file_path):
        return read_tsv_file(file_path)

    # Run the flow
    records = run_flow_and_capture_logs(test_flow, mock_tsv_file).result

    # Check that we got the expected number of records
    assert len(records) == 5

    # Check the content of the first record
    assert records[0]["primary_assembly"] == "ERZ123456"
    assert records[0]["genome"] == "MGYG000000001"
    assert records[0]["mag_accession"] == "MAGS12345"
    assert records[0]["species_rep"] == "GCA_123456789.1"


@pytest.mark.django_db
def test_process_tsv_records():
    """Test the process_tsv_records task."""
    # Create test records
    records = [
        {
            "primary_assembly": "ERZ123456",
            "genome": "MGYG000000001",
            "mag_accession": "MAGS12345",
            "species_rep": "GCA_123456789.1",
        },
        {
            "primary_assembly": "ERZ789012",
            "genome": "MGYG000000002",
            "mag_accession": "MAGS67890",
            "species_rep": "GCA_987654321.1",
        },
        {
            "primary_assembly": "ERZ111111",
            "genome": "MGYG000000003",
            "mag_accession": None,
            "species_rep": None,
        },
        {
            "primary_assembly": "",  # Missing required field
            "genome": "MGYG000000004",
            "mag_accession": "MAGS11111",
            "species_rep": "GCA_111111111.1",
        },
        {
            "primary_assembly": "ERZ222222",
            "genome": "",  # Missing required field
            "mag_accession": "MAGS22222",
            "species_rep": "GCA_222222222.1",
        },
    ]

    # Create a test flow that calls the process_tsv_records task
    @flow(name="Test Process TSV Records")
    def test_flow(input_records):
        return process_tsv_records(input_records)

    # Run the flow
    validated_records = run_flow_and_capture_logs(test_flow, records).result

    # Check that records with missing required fields were filtered out
    assert len(validated_records) == 3

    # Check that the records were cleaned properly
    assert validated_records[0]["primary_assembly"] == "ERZ123456"
    assert validated_records[0]["genome"] == "MGYG000000001"
    assert validated_records[0]["mag_accession"] == "MAGS12345"
    assert validated_records[0]["species_rep"] == "GCA_123456789.1"

    # Check that records with None values for optional fields are preserved
    assert validated_records[2]["primary_assembly"] == "ERZ111111"
    assert validated_records[2]["genome"] == "MGYG000000003"
    assert validated_records[2]["mag_accession"] is None
    assert validated_records[2]["species_rep"] is None


@pytest.mark.django_db
def test_find_objects():
    """Test the find_objects task."""
    # Create test biome
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )

    # Create test catalogue
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )

    # Create test genomes
    genome1 = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type=Genome.MAG,
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

    # Create ENA study
    ena_study = ENAStudy.objects.create(
        accession="PRJEB12345",
        title="Test ENA Study",
    )

    # Create study
    study = Study.objects.create(
        accession="MGYS00000001",
        ena_study=ena_study,
        title="Test Study",
    )

    # Create ENA sample
    ena_sample = ENASample.objects.create(
        accession="ERS123456",
        study=ena_study,
    )

    # Create sample
    sample = Sample.objects.create(
        ena_sample=ena_sample,
        ena_study=ena_study,
    )
    sample.studies.add(study)

    # Create run
    run = Run.objects.create(
        study=study,
        sample=sample,
        ena_study=ena_study,
    )

    # Create assembly
    assembly1 = Assembly.objects.create(
        run=run,
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )
    assembly1.ena_accessions = ["ERZ123456"]
    assembly1.save()

    # Create test records
    records = [
        {
            "primary_assembly": "ERZ123456",
            "genome": "MGYG000000001",
            "mag_accession": "MAGS12345",
            "species_rep": "GCA_123456789.1",
        },
        {
            "primary_assembly": "ERZ789012",  # Non-existent assembly
            "genome": "MGYG000000001",
            "mag_accession": "MAGS67890",
            "species_rep": "GCA_987654321.1",
        },
        {
            "primary_assembly": "ERZ123456",
            "genome": "MGYG000000002",  # Non-existent genome
            "mag_accession": None,
            "species_rep": None,
        },
    ]

    # Mock the get_by_accession method
    with patch(
        "analyses.models.Assembly.objects.get_by_accession"
    ) as mock_get_by_accession:

        def mock_get_by_accession_side_effect(accession):
            if accession == "ERZ123456":
                return assembly1
            raise ObjectDoesNotExist()

        mock_get_by_accession.side_effect = mock_get_by_accession_side_effect

        # Create a test flow that calls the find_objects task
        @flow(name="Test Find Objects")
        def test_flow(input_records):
            return find_objects(input_records)

        # Run the flow
        results = run_flow_and_capture_logs(test_flow, records).result

        # Check that we got the expected number of results
        assert len(results) == 3

        # Check the content of the results
        assert results[0][0] == records[0]  # Record
        assert results[0][1] == genome1  # Genome
        assert results[0][2] == assembly1  # Assembly

        assert results[1][0] == records[1]  # Record
        assert results[1][1] == genome1  # Genome
        assert results[1][2] is None  # Assembly not found

        assert results[2][0] == records[2]  # Record
        assert results[2][1] is None  # Genome not found
        assert results[2][2] == assembly1  # Assembly


@pytest.mark.django_db
def test_create_links():
    """Test the create_links task."""
    # Create test biome
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )

    # Create test catalogue
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )

    # Create test genomes
    genome1 = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type=Genome.MAG,
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

    genome2 = Genome.objects.create(
        accession="MGYG000000002",
        biome=biome,
        length=2000000,
        num_contigs=200,
        n_50=20000,
        gc_content=0.6,
        type=Genome.MAG,
        completeness=90.0,
        contamination=3.0,
        trnas=25.0,
        nc_rnas=15,
        num_proteins=2000,
        eggnog_coverage=85.0,
        ipr_coverage=80.0,
        taxon_lineage="Bacteria;Firmicutes;Bacilli",
        catalogue=catalogue,
    )

    # Create ENA study
    ena_study = ENAStudy.objects.create(
        accession="PRJEB12345",
        title="Test ENA Study",
    )

    # Create study
    study = Study.objects.create(
        accession="MGYS00000001",
        ena_study=ena_study,
        title="Test Study",
    )

    # Create ENA sample
    ena_sample = ENASample.objects.create(
        accession="ERS123456",
        study=ena_study,
    )

    # Create sample
    sample = Sample.objects.create(
        ena_sample=ena_sample,
        ena_study=ena_study,
    )
    sample.studies.add(study)

    # Create run
    run = Run.objects.create(
        study=study,
        sample=sample,
        ena_study=ena_study,
    )

    # Create assemblies
    assembly1 = Assembly.objects.create(
        run=run,
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )

    assembly2 = Assembly.objects.create(
        run=run,
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )

    # Create test objects
    objects = [
        (
            {
                "primary_assembly": "ERZ123456",
                "genome": "MGYG000000001",
                "mag_accession": "MAGS12345",
                "species_rep": "GCA_123456789.1",
            },
            genome1,
            assembly1,
        ),
        (
            {
                "primary_assembly": "ERZ789012",
                "genome": "MGYG000000002",
                "mag_accession": "MAGS67890",
                "species_rep": "GCA_987654321.1",
            },
            genome2,
            assembly2,
        ),
        (
            {
                "primary_assembly": "ERZ111111",
                "genome": "MGYG000000003",  # Non-existent genome
                "mag_accession": None,
                "species_rep": None,
            },
            None,
            assembly1,
        ),
        (
            {
                "primary_assembly": "ERZ222222",  # Non-existent assembly
                "genome": "MGYG000000001",
                "mag_accession": "MAGS22222",
                "species_rep": "GCA_222222222.1",
            },
            genome1,
            None,
        ),
    ]

    # Create a link that already exists
    existing_link = GenomeAssemblyLink.objects.create(
        genome=genome1,
        assembly=assembly1,
        species_rep="OLD_REP",
        mag_accession="OLD_MAG",
    )

    # Create a test flow that calls the create_links task
    @flow(name="Test Create Links")
    def test_flow(input_objects):
        return create_links(input_objects)

    # Run the flow
    links_count = run_flow_and_capture_logs(test_flow, objects).result

    # Check that we created/updated the expected number of links
    assert links_count == 2

    # Check that the existing link was updated
    existing_link.refresh_from_db()
    assert existing_link.species_rep == "GCA_123456789.1"
    assert existing_link.mag_accession == "MAGS12345"

    # Check that a new link was created
    new_link = GenomeAssemblyLink.objects.get(genome=genome2, assembly=assembly2)
    assert new_link.species_rep == "GCA_987654321.1"
    assert new_link.mag_accession == "MAGS67890"

    # Check that no link was created for objects with missing genome or assembly
    assert GenomeAssemblyLink.objects.count() == 2


@pytest.mark.django_db
def test_import_genome_assembly_links_flow(mock_tsv_file):
    """Test the import_genome_assembly_links flow end-to-end."""
    # Create test biome
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )

    # Create test catalogue
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )

    # Create test genomes
    genome1 = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type=Genome.MAG,
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

    genome2 = Genome.objects.create(
        accession="MGYG000000002",
        biome=biome,
        length=2000000,
        num_contigs=200,
        n_50=20000,
        gc_content=0.6,
        type=Genome.MAG,
        completeness=90.0,
        contamination=3.0,
        trnas=25.0,
        nc_rnas=15,
        num_proteins=2000,
        eggnog_coverage=85.0,
        ipr_coverage=80.0,
        taxon_lineage="Bacteria;Firmicutes;Bacilli",
        catalogue=catalogue,
    )

    # Create ENA study
    ena_study = ENAStudy.objects.create(
        accession="PRJEB12345",
        title="Test ENA Study",
    )

    # Create study
    study = Study.objects.create(
        accession="MGYS00000001",
        ena_study=ena_study,
        title="Test Study",
    )

    # Create ENA sample
    ena_sample = ENASample.objects.create(
        accession="ERS123456",
        study=ena_study,
    )

    # Create sample
    sample = Sample.objects.create(
        ena_sample=ena_sample,
        ena_study=ena_study,
    )
    sample.studies.add(study)

    # Create run
    run = Run.objects.create(
        study=study,
        sample=sample,
        ena_study=ena_study,
    )

    # Create assemblies
    assembly1 = Assembly.objects.create(
        run=run,
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )
    assembly1.ena_accessions = ["ERZ123456"]
    assembly1.save()

    assembly2 = Assembly.objects.create(
        run=run,
        sample=sample,
        reads_study=study,
        ena_study=ena_study,
    )
    assembly2.ena_accessions = ["ERZ789012"]
    assembly2.save()

    # Mock the get_by_accession method
    with patch(
        "analyses.models.Assembly.objects.get_by_accession"
    ) as mock_get_by_accession:
        mock_get_by_accession.side_effect = (
            lambda accession: {
                "ERZ123456": assembly1,
                "ERZ789012": assembly2,
            }.get(accession, None)
            or ObjectDoesNotExist()
        )

        # Run the flow
        logged_flow_run = run_flow_and_capture_logs(
            import_genome_assembly_links,
            mock_tsv_file,
        )

        # Check the result
        result = logged_flow_run.result
        assert result["total_records"] == 5
        assert result["validated_records"] == 3
        assert result["links_created_or_updated"] == 2

        # Check that the links were created
        assert GenomeAssemblyLink.objects.count() == 2

        link1 = GenomeAssemblyLink.objects.get(genome=genome1, assembly=assembly1)
        assert link1.species_rep == "GCA_123456789.1"
        assert link1.mag_accession == "MAGS12345"

        link2 = GenomeAssemblyLink.objects.get(genome=genome2, assembly=assembly2)
        assert link2.species_rep == "GCA_987654321.1"
        assert link2.mag_accession == "MAGS67890"

        # Check the logs
        assert "Starting import of genome assembly links" in logged_flow_run.logs
        assert "TSV file validated" in logged_flow_run.logs
        assert "Read 5 records from TSV file" in logged_flow_run.logs
        assert "Validated 3 records" in logged_flow_run.logs
        assert "Found objects for 3 records" in logged_flow_run.logs
        assert "Created 2 links" in logged_flow_run.logs
        assert "Import completed" in logged_flow_run.logs
