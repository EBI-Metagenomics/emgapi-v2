from unittest.mock import patch, MagicMock

import pytest

from analyses.models import Biome
from workflows.flows.import_genomes_flow import (
    import_genomes_flow,
    validate_pipeline_version,
    parse_options,
    get_catalogue,
    gather_genome_dirs
)
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


def test_validate_pipeline_version():
    assert validate_pipeline_version.fn("v1") == 1
    assert validate_pipeline_version.fn("v2.0") == 2
    assert validate_pipeline_version.fn("v3.0.0") == 3
    assert validate_pipeline_version.fn("v3.0.0-dev") == 3
    
    with pytest.raises(ValueError):
        validate_pipeline_version.fn("invalid")
    with pytest.raises(ValueError):
        validate_pipeline_version.fn("v4")


def test_parse_options():
    with patch('os.path.exists', return_value=True):
        options = {
            'results_directory': '/path/to/results',
            'catalogue_directory': 'catalogue',
            'catalogue_name': 'Test Catalogue',
            'catalogue_version': '1.0',
            'gold_biome': 'root:Host-Associated:Human:Digestive System',
            'pipeline_version': 'v1',
            'catalogue_type': 'prokaryotes',
        }
        parsed_options = parse_options.fn(options)
        assert parsed_options['catalogue_dir'] == '/path/to/results/catalogue'
        assert parsed_options['catalogue_name'] == 'Test Catalogue'
        assert parsed_options['catalogue_version'] == '1.0'
        assert parsed_options['gold_biome'] == 'root:Host-Associated:Human:Digestive System'
        assert parsed_options['pipeline_version'] == 'v1'
        assert parsed_options['catalogue_type'] == 'prokaryotes'
        assert parsed_options['catalogue_biome_label'] == 'Test Catalogue'
        assert parsed_options['database'] == 'default'
    
    with patch('os.path.exists', return_value=False):
        options = {
            'results_directory': '/path/to/results',
            'catalogue_directory': 'catalogue',
            'catalogue_name': 'Test Catalogue',
            'catalogue_version': '1.0',
            'gold_biome': 'root:Host-Associated:Human:Digestive System',
            'pipeline_version': 'v1',
            'catalogue_type': 'prokaryotes',
        }
        with pytest.raises(FileNotFoundError):
            parse_options.fn(options)


@pytest.mark.django_db
def test_get_catalogue():
    biome = Biome.objects.create(
        biome_id=1,
        biome_name="Test Biome",
        lineage="root:Host-Associated:Human:Digestive System",
        depth=4
    )
    
    options = {
        'catalogue_name': 'Test Catalogue',
        'catalogue_version': '1.0',
        'gold_biome': 'root:Host-Associated:Human:Digestive System',
        'catalogue_dir': '/path/to/catalogue',
        'pipeline_version': 'v1',
        'catalogue_type': 'prokaryotes',
        'catalogue_biome_label': 'Test Catalogue',
        'database': 'default'
    }
    
    with patch('analyses.models.Biome.lineage_to_path', return_value=biome.path):
        catalogue = get_catalogue.fn(options)
        
        assert catalogue.catalogue_id == 'test-catalogue-v1-0'
        assert catalogue.version == '1.0'
        assert catalogue.name == 'Test Catalogue v1.0'
        assert catalogue.biome == biome
        assert catalogue.result_directory == '/path/to/catalogue'
        assert catalogue.pipeline_version_tag == 'v1'
        assert catalogue.catalogue_biome_label == 'Test Catalogue'
        assert catalogue.catalogue_type == 'prokaryotes'


@pytest.mark.django_db
@patch('workflows.flows.import_genomes_flow.find_genome_results')
@patch('workflows.flows.import_genomes_flow.sanity_check_genome_output_proks')
@patch('workflows.flows.import_genomes_flow.sanity_check_catalogue_dir')
def test_gather_genome_dirs(mock_sanity_check_catalogue, mock_sanity_check_proks, mock_find_genome_results):
    mock_find_genome_results.return_value = ['/path/to/genome1', '/path/to/genome2']
    
    dirs = gather_genome_dirs.fn('/path/to/catalogue', 'prokaryotes')
    assert dirs == ['/path/to/genome1', '/path/to/genome2']
    mock_find_genome_results.assert_called_once_with('/path/to/catalogue')
    mock_sanity_check_proks.assert_called()
    mock_sanity_check_catalogue.assert_called_once_with('/path/to/catalogue')
    
    mock_find_genome_results.reset_mock()
    mock_sanity_check_proks.reset_mock()
    mock_sanity_check_catalogue.reset_mock()
    
    with patch('workflows.flows.import_genomes_flow.sanity_check_genome_output_euks') as mock_sanity_check_euks:
        dirs = gather_genome_dirs.fn('/path/to/catalogue', 'eukaryotes')
        assert dirs == ['/path/to/genome1', '/path/to/genome2']
        mock_find_genome_results.assert_called_once_with('/path/to/catalogue')
        mock_sanity_check_euks.assert_called()
        mock_sanity_check_catalogue.assert_called_once_with('/path/to/catalogue')


@pytest.mark.django_db
@patch('workflows.flows.import_genomes_flow.process_genome_dir')
@patch('workflows.flows.import_genomes_flow.gather_genome_dirs')
@patch('workflows.flows.import_genomes_flow.get_catalogue')
@patch('workflows.flows.import_genomes_flow.parse_options')
def test_import_genomes_flow(mock_parse_options, mock_get_catalogue, mock_gather_genome_dirs, mock_process_genome_dir):
    mock_parse_options.return_value = {
        'results_directory': '/path/to/results',
        'catalogue_dir': '/path/to/catalogue',
        'catalogue_name': 'Test Catalogue',
        'catalogue_version': '1.0',
        'gold_biome': 'root:Host-Associated:Human:Digestive System',
        'pipeline_version': 'v1',
        'catalogue_type': 'prokaryotes',
        'catalogue_biome_label': 'Test Catalogue',
        'database': 'default'
    }
    mock_catalogue = MagicMock()
    mock_get_catalogue.return_value = mock_catalogue
    mock_gather_genome_dirs.return_value = ['/path/to/genome1', '/path/to/genome2']
    mock_process_genome_dir.return_value = 'MGYG000000001'
    
    flow_run = run_flow_and_capture_logs(
        import_genomes_flow,
        options={
            'results_directory': '/path/to/results',
            'catalogue_directory': 'catalogue',
            'catalogue_name': 'Test Catalogue',
            'catalogue_version': '1.0',
            'gold_biome': 'root:Host-Associated:Human:Digestive System',
            'pipeline_version': 'v1',
            'catalogue_type': 'prokaryotes',
        }
    )
    
    assert "Final Report:" in flow_run.logs
    
    mock_parse_options.assert_called_once()
    mock_get_catalogue.assert_called_once()
    mock_gather_genome_dirs.assert_called_once()