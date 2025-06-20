import os
import pytest
from unittest.mock import patch, MagicMock
from django.core.management import call_command
from django.core.management.base import CommandError

from analyses.models import Biome
from genomes.models import GenomeCatalogue, Genome, GeographicLocation
from genomes.management.commands.import_genomes import Command


@pytest.mark.django_db
class TestImportGenomesCommand:
    
    def setup_method(self):
        # Create a test biome
        self.biome = Biome.objects.create(
            biome_id=1,
            biome_name="Test Biome",
            lineage="root:Host-Associated:Human:Digestive System",
            depth=4
        )
        
    @patch('os.path.exists')
    @patch('genomes.management.commands.import_genomes.find_genome_results')
    @patch('genomes.management.commands.import_genomes.sanity_check_catalogue_dir')
    def test_command_basic_execution(self, mock_sanity_check_catalogue, mock_find_genome_results, mock_path_exists):
        # Setup mocks
        mock_path_exists.return_value = True
        mock_find_genome_results.return_value = []
        
        # Create a mock for Biome.lineage_to_path
        with patch('analyses.models.Biome.lineage_to_path', return_value=self.biome.path):
            # Call the command
            call_command(
                'import_genomes',
                '/path/to/results',
                'catalogue',
                'Test Catalogue',
                '1.0',
                'root:Host-Associated:Human:Digestive System',
                'v1',
                'prokaryotes'
            )
            
            # Verify the catalogue was created
            catalogue = GenomeCatalogue.objects.get(catalogue_id='test-catalogue-v1-0')
            assert catalogue.version == '1.0'
            assert catalogue.name == 'Test Catalogue v1.0'
            assert catalogue.biome == self.biome
            assert catalogue.pipeline_version_tag == 'v1'
            assert catalogue.catalogue_type == 'prokaryotes'
    
    @patch('os.path.exists')
    def test_command_invalid_results_directory(self, mock_path_exists):
        # Setup mock to return False for the results directory
        mock_path_exists.return_value = False
        
        # Call the command and expect a FileNotFoundError
        with pytest.raises(FileNotFoundError):
            call_command(
                'import_genomes',
                '/path/to/nonexistent',
                'catalogue',
                'Test Catalogue',
                '1.0',
                'root:Host-Associated:Human:Digestive System',
                'v1',
                'prokaryotes'
            )
    
    @patch('os.path.exists')
    def test_command_invalid_pipeline_version(self, mock_path_exists):
        # Setup mock to return True for the results directory
        mock_path_exists.return_value = True
        
        # Call the command with an invalid pipeline version and expect a CommandError
        with pytest.raises(CommandError):
            call_command(
                'import_genomes',
                '/path/to/results',
                'catalogue',
                'Test Catalogue',
                '1.0',
                'root:Host-Associated:Human:Digestive System',
                'v4',  # Invalid version
                'prokaryotes'
            )
    
    @patch('os.path.exists')
    @patch('genomes.management.commands.import_genomes.find_genome_results')
    @patch('genomes.management.commands.import_genomes.sanity_check_catalogue_dir')
    @patch('genomes.management.commands.import_genomes.Command.upload_dir')
    def test_command_with_genome_dirs(self, mock_upload_dir, mock_sanity_check_catalogue, 
                                      mock_find_genome_results, mock_path_exists):
        # Setup mocks
        mock_path_exists.return_value = True
        mock_find_genome_results.return_value = ['/path/to/genome1', '/path/to/genome2']
        
        # Create a mock for Biome.lineage_to_path
        with patch('analyses.models.Biome.lineage_to_path', return_value=self.biome.path):
            # Call the command
            call_command(
                'import_genomes',
                '/path/to/results',
                'catalogue',
                'Test Catalogue',
                '1.0',
                'root:Host-Associated:Human:Digestive System',
                'v1',
                'prokaryotes'
            )
            
            # Verify upload_dir was called for each genome directory
            assert mock_upload_dir.call_count == 2
    
    @patch('os.path.exists')
    @patch('genomes.management.commands.import_genomes.find_genome_results')
    @patch('genomes.management.commands.import_genomes.sanity_check_catalogue_dir')
    @patch('genomes.management.commands.import_genomes.Command.upload_dir')
    @patch('genomes.management.commands.import_genomes.Command.upload_catalogue_files')
    def test_command_with_update_metadata_only(self, mock_upload_catalogue_files, mock_upload_dir, 
                                              mock_sanity_check_catalogue, mock_find_genome_results, 
                                              mock_path_exists):
        # Setup mocks
        mock_path_exists.return_value = True
        mock_find_genome_results.return_value = ['/path/to/genome1', '/path/to/genome2']
        
        # Create a catalogue first
        catalogue = GenomeCatalogue.objects.create(
            catalogue_id='test-catalogue-v1-0',
            version='1.0',
            name='Test Catalogue v1.0',
            biome=self.biome,
            pipeline_version_tag='v1',
            catalogue_type='prokaryotes'
        )
        
        # Create a mock for Biome.lineage_to_path
        with patch('analyses.models.Biome.lineage_to_path', return_value=self.biome.path):
            # Call the command with update_metadata_only
            call_command(
                'import_genomes',
                '/path/to/results',
                'catalogue',
                'Test Catalogue',
                '1.0',
                'root:Host-Associated:Human:Digestive System',
                'v1',
                'prokaryotes',
                '--update-metadata-only'
            )
            
            # Verify upload_dir was called with update_metadata_only=True
            mock_upload_dir.assert_called_with('/path/to/genome1', update_metadata_only=True)