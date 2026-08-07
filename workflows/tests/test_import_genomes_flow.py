import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import Biome
from genomes.models import CatalogueGenome, GenomeCatalogue

genome_config = settings.EMG_CONFIG.genomes


@pytest.fixture
def mock_genome_directory():
    """
    Create a temporary directory structure that mimics the expected genome directory structure.
    This allows tests to run without needing the actual large directory structure.
    """
    temp_dir = tempfile.mkdtemp()

    try:
        catalogue_dir = Path(temp_dir) / "sheep-rumen" / "1.0" / "website"
        catalogue_dir.mkdir(parents=True, exist_ok=True)

        with (catalogue_dir / "phylo_tree.json").open("w") as f:
            json.dump({"tree": "mock_tree_data"}, f)

        with (catalogue_dir / "catalogue_summary.json").open("w") as f:
            json.dump({"summary": "mock_summary_data"}, f)

        genome_accessions = ["MGYG000000001", "MGYG000000002"]
        for accession in genome_accessions:
            genome_dir = catalogue_dir / accession
            genome_dir.mkdir(parents=True, exist_ok=True)
            (genome_dir / "genome").mkdir(parents=True, exist_ok=True)

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
                "pangenome": {
                    "geographic_range": ["Europe", "Africa"],
                    "num_genomes_non_redundant": 5,  # is deprecated
                    "num_genomes_total": 6,
                    "pangenome_accessory_size": 1576,
                    "pangenome_core_size": 3041,
                    "pangenome_size": 4617,
                },
            }

            with (genome_dir / f"{accession}.json").open("w") as f:
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
        parent = str(Path(catalogue_dir).parent)
        yield parent
    finally:
        shutil.rmtree(temp_dir)


from workflows.flows.import_genomes_flow import (
    gather_genome_dirs,
    get_catalogue,
    import_genomes_flow,
    move_catalogue_files_to_web_results,
    parse_options,
    validate_pipeline_version,
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
        assert (parsed_options["catalogue_name"]) == "Sheep rumen"
        assert parsed_options["catalogue_version"] == "1.0"
        assert parsed_options["gold_biome"] == "root:Host-associated:Human"
        assert parsed_options["pipeline_version"] == "v3.0.0dev"
        assert parsed_options["catalogue_type"] == "prokaryotes"
        assert parsed_options["catalogue_biome_label"] == "Sheep Rumen"
        assert parsed_options["destination_dir_name"] == "sheep-rumen"
        assert parsed_options["catalogue_slug"] == "sheep-rumen-v1-0"

        options = get_default_options(catalogue_biome_label=None)
        parsed_options = parse_options(options)
        assert parsed_options["catalogue_biome_label"] == "Sheep rumen"

    with patch("os.path.exists", return_value=False):
        options = get_default_options()
        with pytest.raises(FileNotFoundError):
            parse_options(options)


def test_move_catalogue_files_to_web_results_uses_slurm_ftp_results_dir():
    options = {
        "results_directory": "/nfs/donco/results/soil/1.0",
        "destination_dir_name": "soil",
        "catalogue_version": "1.0",
    }

    with (
        patch(
            "workflows.flows.import_genomes_flow.EMG_CONFIG.slurm.ftp_results_dir",
            "/nfs/ftp/public/databases/metagenomics/mgnify_results",
        ),
        patch("workflows.flows.import_genomes_flow.get_run_logger") as logger,
        patch("workflows.flows.import_genomes_flow.run_deployment") as run_deployment,
    ):
        run_deployment.return_value = "flow-run"

        move_catalogue_files_to_web_results.fn(options)

    run_deployment.assert_called_once()
    assert run_deployment.call_args.kwargs["parameters"]["source"] == (
        "/nfs/donco/results/soil/1.0/website/"
    )
    assert run_deployment.call_args.kwargs["parameters"]["target"] == (
        "/nfs/ftp/public/databases/metagenomics/mgnify_results/"
        "mgnify_genomes/soil/1.0"
    )
    logger.return_value.info.assert_called_once_with(
        "Web results mover flowrun is flow-run"
    )


@pytest.mark.django_db
def test_get_catalogue():

    biome = Biome.objects.create(
        id=1,
        biome_name="Rumen",
        path="root.host_associated.mammals.digestive_system.stomach.rumen",
    )

    options = get_default_options()

    with patch("analyses.models.Biome.lineage_to_path", return_value=biome.path):
        catalogue = get_catalogue(options)

        assert catalogue.catalogue_id == "sheep-rumen-v1-0"
        assert catalogue.version == "1.0"
        assert catalogue.name == "Sheep rumen v1.0"
        assert catalogue.biome == biome
        assert catalogue.result_directory == "/mgnify_genomes/sheep-rumen/1.0"
        assert catalogue.pipeline_version_tag == "v3.0.0dev"
        assert catalogue.catalogue_biome_label == "Sheep Rumen"
        assert catalogue.catalogue_type == "prokaryotes"
        assert catalogue.status == GenomeCatalogue.Status.DRAFT

        catalogue.status = GenomeCatalogue.Status.PUBLISHED
        catalogue.save(update_fields=["status"])
        with pytest.raises(ValueError, match="cannot be re-imported"):
            get_catalogue(options)


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


@pytest.mark.django_db(transaction=True)
@patch("analyses.models.Biome.lineage_to_path")
def test_import_genomes_flow_with_mock_directory(
    mock_lineage_to_path,
    mock_genome_directory,
):
    """
    Test the import_genomes_flow using a mock directory structure.

    This test creates a temporary directory with the required structure and files,
    and then runs the flow with this directory as the results directory.

    """
    biome = Biome.objects.create(
        id=1,
        biome_name="Rumen",
        path="root.host_associated.mammals.digestive_system.stomach.rumen",
    )
    mock_lineage_to_path.return_value = biome.path

    options = get_default_options(mock_genome_directory)
    run_flow_and_capture_logs(
        import_genomes_flow,
        results_directory=options["results_directory"],
        catalogue_name=options["catalogue_name"],
        catalogue_version=options["catalogue_version"],
        gold_biome=options["gold_biome"],
        pipeline_version=options["pipeline_version"],
        catalogue_type=options["catalogue_type"],
        catalogue_biome_label=options["catalogue_biome_label"],
    )
    catalogue = GenomeCatalogue.objects.get(catalogue_id="sheep-rumen-v1-0")
    assert catalogue.version == "1.0"
    assert catalogue.name == "Sheep rumen v1.0"
    assert catalogue.biome == biome
    assert catalogue.status == GenomeCatalogue.Status.READY

    genomes = CatalogueGenome.objects.filter(catalogue=catalogue)
    assert genomes.count() == 2

    accessions = [g.accession for g in genomes]
    assert "MGYG000000001" in accessions
    assert "MGYG000000002" in accessions

    genome = CatalogueGenome.objects.get(
        catalogue=catalogue, genome__accession="MGYG000000001"
    )
    assert genome.num_genomes_total == 6
    assert genome.pangenome_size == 4617
    assert genome.pangenome_core_size == 3041
    assert genome.pangenome_accessory_size == 1576
    assert genome.geographic_range == ["Europe", "Africa"]
    assert catalogue.result_directory == "/mgnify_genomes/sheep-rumen/1.0"
    assert genome.result_directory == "/mgnify_genomes/sheep-rumen/1.0/MGYG000000001"


@pytest.mark.django_db(transaction=True)
@patch("analyses.models.Biome.lineage_to_path")
def test_failed_import_remains_draft(mock_lineage_to_path, mock_genome_directory):
    biome = Biome.objects.create(
        id=1,
        biome_name="Rumen",
        path="root.host_associated.mammals.digestive_system.stomach.rumen",
    )
    mock_lineage_to_path.return_value = biome.path
    options = get_default_options(mock_genome_directory)

    with patch(
        "workflows.flows.import_genomes_flow.process_genome_dir",
        side_effect=ValueError("Broken genome"),
    ):
        state = import_genomes_flow(
            results_directory=options["results_directory"],
            catalogue_name=options["catalogue_name"],
            catalogue_version=options["catalogue_version"],
            gold_biome=options["gold_biome"],
            pipeline_version=options["pipeline_version"],
            catalogue_type=options["catalogue_type"],
            catalogue_biome_label=options["catalogue_biome_label"],
            return_state=True,
        )

    assert state.is_failed()
    catalogue = GenomeCatalogue.objects.get(catalogue_id="sheep-rumen-v1-0")
    assert catalogue.status == GenomeCatalogue.Status.DRAFT


def get_default_options(
    results_directory: str = "genomes/sheep-rumen/1.0",
    catalogue_name: str = "Sheep rumen",
    catalogue_version: str = "1.0",
    gold_biome: str = "root:Host-associated:Human",
    pipeline_version: str = "v3.0.0dev",
    catalogue_type: str = "prokaryotes",
    catalogue_biome_label: str = "Sheep Rumen",
    destination_dir_name: str = None,
    catalogue_slug: str = None,
):
    return {
        "results_directory": results_directory,
        "catalogue_dir": "genomes/sheep-rumen/1.0",
        "catalogue_name": catalogue_name,
        "catalogue_version": catalogue_version,
        "gold_biome": gold_biome,
        "pipeline_version": pipeline_version,
        "catalogue_type": catalogue_type,
        "catalogue_biome_label": catalogue_biome_label,
        "destination_dir_name": destination_dir_name,
        "catalogue_slug": catalogue_slug,
    }
