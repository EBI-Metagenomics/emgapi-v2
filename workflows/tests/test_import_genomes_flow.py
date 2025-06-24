import os
import json
import tempfile
import shutil
from unittest.mock import patch

import pytest

from analyses.models import Biome
from genomes.models import GenomeCatalogue, Genome


@pytest.fixture
def mock_genome_directory():
    """
    Create a temporary directory structure that mimics the expected genome directory structure.
    This allows tests to run without needing the actual large directory structure.
    """
    temp_dir = tempfile.mkdtemp()

    try:
        catalogue_dir = os.path.join(temp_dir, "catalogue")
        os.makedirs(catalogue_dir)

        with open(os.path.join(catalogue_dir, "phylo_tree.json"), "w") as f:
            json.dump({"tree": "mock_tree_data"}, f)

        with open(os.path.join(catalogue_dir, "catalogue_summary.json"), "w") as f:
            json.dump({"summary": "mock_summary_data"}, f)

        genome_accessions = ["MGYG000000001", "MGYG000000002"]
        for accession in genome_accessions:
            genome_dir = os.path.join(catalogue_dir, accession)
            os.makedirs(genome_dir)

            os.makedirs(os.path.join(genome_dir, "genome"))

            json_data = {
                "accession": accession,
                "completeness": 95.0,
                "contamination": 1.2,
                "eggnog_coverage": 80.0,
                "gc_content": 45.0,
                "gold_biome": "root:Host-Associated:Human:Digestive System",
                "ipr_coverage": 75.0,
                "length": 3000000,
                "n_50": 50000,
                "nc_rnas": 10,
                "num_contigs": 100,
                "num_proteins": 3000,
                "rna_16s": 1.0,
                "rna_23s": 1.0,
                "rna_5s": 1.0,
                "taxon_lineage": "d__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales;f__Lactobacillaceae;g__Lactobacillus;s__Lactobacillus_gasseri",
                "trnas": 40.0,
                "type": "mag",
            }

            with open(os.path.join(genome_dir, f"{accession}.json"), "w") as f:
                json.dump(json_data, f)

            genome_subdir = os.path.join(genome_dir, "genome")
            required_files = [
                f"{accession}_annotation_coverage.tsv",
                f"{accession}_cazy_summary.tsv",
                f"{accession}_cog_summary.tsv",
                f"{accession}_kegg_classes.tsv",
                f"{accession}_kegg_modules.tsv",
                f"{accession}.faa",
                f"{accession}.fna",
                f"{accession}.fna.fai",
                f"{accession}.gff",
                f"{accession}_eggNOG.tsv",
                f"{accession}_InterProScan.tsv",
            ]

            for file in required_files:
                with open(os.path.join(genome_subdir, file), "w") as f:
                    f.write(f"Mock content for {file}")

        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


from workflows.flows.import_genomes_flow import (
    import_genomes_flow,
    validate_pipeline_version,
    parse_options,
    get_catalogue,
    gather_genome_dirs,
)
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


def test_validate_pipeline_version():
    assert validate_pipeline_version("v1") == 1
    assert validate_pipeline_version("v2.0") == 2
    assert validate_pipeline_version("v3.0.0") == 3
    assert validate_pipeline_version("v3.0.0-dev") == 3

    with pytest.raises(ValueError):
        validate_pipeline_version("invalid")
    with pytest.raises(ValueError):
        validate_pipeline_version("v4")


def test_parse_options():
    with patch("os.path.exists", return_value=True):
        options = get_default_options()
        parsed_options = parse_options(options)
        assert parsed_options["catalogue_directory"] == "genomes/sheep-rumen/1.0"
        assert (parsed_options["catalogue_name"]) == "Sheep rumen"
        assert parsed_options["catalogue_version"] == "1.0"
        assert parsed_options["gold_biome"] == "root:Host-associated:Human"
        assert parsed_options["pipeline_version"] == "v3.0.0dev"
        assert parsed_options["catalogue_type"] == "prokaryotes"
        assert parsed_options["catalogue_biome_label"] == "Sheep Rumen"
        assert parsed_options["database"] == "default"

    with patch("os.path.exists", return_value=False):
        options = get_default_options()
        with pytest.raises(FileNotFoundError):
            parse_options(options)


@pytest.mark.django_db
def test_get_catalogue():

    biome = Biome.objects.create(
        id=1,
        biome_name="Rumen",
        # path="root:Host-Associated:Human:Digestive System",
        path="root.host_associated.mammals.digestive_system.stomach.rumen",
    )

    options = get_default_options()

    with patch("analyses.models.Biome.lineage_to_path", return_value=biome.path):
        catalogue = get_catalogue(options)

        assert catalogue.catalogue_id == "sheep-rumen-v1-0"
        assert catalogue.version == "1.0"
        assert catalogue.name == "Sheep rumen v1.0"
        assert catalogue.biome == biome
        assert catalogue.result_directory == "genomes/sheep-rumen/1.0"
        assert catalogue.pipeline_version_tag == "v3.0.0dev"
        assert catalogue.catalogue_biome_label == "Sheep Rumen"
        assert catalogue.catalogue_type == "prokaryotes"


@pytest.mark.django_db
@patch("workflows.flows.import_genomes_flow.find_genome_results")
@patch("workflows.flows.import_genomes_flow.sanity_check_genome_output_proks")
@patch("workflows.flows.import_genomes_flow.sanity_check_catalogue_dir")
def test_gather_genome_dirs(
    mock_sanity_check_catalogue, mock_sanity_check_proks, mock_find_genome_results
):
    mock_find_genome_results.return_value = ["/path/to/genome1", "/path/to/genome2"]

    dirs = gather_genome_dirs("/path/to/catalogue", "prokaryotes")
    assert dirs == ["/path/to/genome1", "/path/to/genome2"]
    mock_find_genome_results.assert_called_once_with("/path/to/catalogue")
    mock_sanity_check_proks.assert_called()
    mock_sanity_check_catalogue.assert_called_once_with("/path/to/catalogue")

    mock_find_genome_results.reset_mock()
    mock_sanity_check_proks.reset_mock()
    mock_sanity_check_catalogue.reset_mock()

    with patch(
        "workflows.flows.import_genomes_flow.sanity_check_genome_output_euks"
    ) as mock_sanity_check_euks:
        dirs = gather_genome_dirs("/path/to/catalogue", "eukaryotes")
        assert dirs == ["/path/to/genome1", "/path/to/genome2"]
        mock_find_genome_results.assert_called_once_with("/path/to/catalogue")
        mock_sanity_check_euks.assert_called()
        mock_sanity_check_catalogue.assert_called_once_with("/path/to/catalogue")


@pytest.mark.django_db
@patch("analyses.models.Biome.lineage_to_path")
def test_import_genomes_flow_with_mock_directory(
    mock_lineage_to_path,
    mock_genome_directory,
):
    """
    Test the import_genomes_flow using a mock directory structure.

    This test creates a temporary directory with the required structure and files,
    and then runs the flow with this directory as the results directory.

    We mock only the necessary functions to avoid database interactions and to focus
    on testing the flow's interaction with the file system.
    """
    # Create a biome for the test
    biome = Biome.objects.create(
        id=1,
        biome_name="Rumen",
        path="root.host_associated.mammals.digestive_system.stomach.rumen",
    )
    mock_lineage_to_path.return_value = biome.path

    options = get_default_options(
        mock_genome_directory, catalogue_directory="catalogue"
    )
    run_flow_and_capture_logs(
        import_genomes_flow,
        results_directory=options["results_directory"],
        catalogue_directory=options["catalogue_directory"],
        catalogue_name=options["catalogue_name"],
        catalogue_version=options["catalogue_version"],
        gold_biome=options["gold_biome"],
        pipeline_version=options["pipeline_version"],
        catalogue_type=options["catalogue_type"],
        catalogue_biome_label=options["catalogue_biome_label"],
        database=options["database"],
    )
    catalogue = GenomeCatalogue.objects.get(catalogue_id="sheep-rumen-v1-0")
    assert catalogue.version == "1.0"
    assert catalogue.name == "Sheep rumen v1.0"
    assert catalogue.biome == biome

    genomes = Genome.objects.filter(catalogue=catalogue)
    assert genomes.count() == 2

    accessions = [g.accession for g in genomes]
    assert "MGYG000000001" in accessions
    assert "MGYG000000002" in accessions


def get_default_options(
    results_directory: str = "/path/to/results",
    catalogue_directory: str = "genomes/sheep-rumen/1.0",
    catalogue_name: str = "Sheep rumen",
    catalogue_version: str = "1.0",
    gold_biome: str = "root:Host-associated:Human",
    pipeline_version: str = "v3.0.0dev",
    catalogue_type: str = "prokaryotes",
    catalogue_biome_label: str = "Sheep Rumen",
    database: str = "default",
):
    return {
        "results_directory": results_directory,
        "catalogue_directory": catalogue_directory,
        "catalogue_dir": catalogue_directory,
        "catalogue_name": catalogue_name,
        "catalogue_version": catalogue_version,
        "gold_biome": gold_biome,
        "pipeline_version": pipeline_version,
        "catalogue_type": catalogue_type,
        "catalogue_biome_label": catalogue_biome_label,
        "database": database,
    }
