from pathlib import Path
from typing import List

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
import analyses.models
from .base import (
    PipelineFileSchema,
    PipelineDirectorySchema,
    PipelineResultSchema,
    DownloadFileMetadata,
)


class MapResultSchema(PipelineResultSchema):
    """Custom MAP result schema with proper file handling."""

    def __init__(self):
        """
        Initialize the MAP pipeline result schema.

        Creates the schema definition for MAP pipeline results.
        """
        # Main mobilome annotation directory (root of MAP output)
        mobilome_annotation_dir = PipelineDirectorySchema(
            folder_name=".",  # MAP outputs directly to the output directory root
            validation_rules=[DirectoryExistsRule],
            files=[
                PipelineFileSchema(
                    filename_template="mobilome_prokka.gff",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        # TODO: we need to centralise models and download groups
                        download_group=analyses.models.Analysis.MAP,
                        short_description="Mobilome annotations",
                        long_description="Mobilome annotation from MAP (Mobilome Annotation Pipeline)",
                    ),
                ),
            ],
        )

        # Initialize parent class
        # TODO: OBTAIN FROM THE SETTINGS
        super().__init__(
            pipeline_name="MAP",
            pipeline_version="1.0",
            directories=[mobilome_annotation_dir],
        )

    def generate_downloads(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> List[DownloadFile]:
        """
        Generate DownloadFile objects for MAP results.

        MAP produces a specific GFF file called 'mobilome_prokka.gff' in the root output directory.

        :param analysis: The analysis object
        :param base_path: Base path containing the pipeline results
        :return: List of DownloadFile objects
        """
        downloads = []
        gff_file = base_path / "mobilome_prokka.gff"

        if gff_file.exists():
            download = DownloadFile(
                path=gff_file.relative_to(analysis.results_dir),
                file_type=DownloadFileType.GFF,
                alias=f"map_{gff_file.name}",
                download_type=DownloadType.GENOME_ANALYSIS,
                download_group="map",
                parent_identifier=analysis.accession,
                short_description="Mobilome annotations",
                long_description="Mobilome annotation from MAP (Mobilome Annotation Pipeline)",
            )
            downloads.append(download)

        return downloads
