from pathlib import Path
from typing import List

from activate_django_first import EMG_CONFIG
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
)
import analyses.models
from .base import (
    PipelineDirectorySchema,
    PipelineResultSchema,
)


class VirifyResultSchema(PipelineResultSchema):
    """Custom VIRify result schema with dynamic file handling."""

    def __init__(self):
        """
        Initialize the VIRify pipeline result schema.

        Creates the schema definition for VIRify pipeline results with dynamic GFF file handling.
        """
        # Main viral annotation directory
        viral_annotation_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.virify_pipeline.final_gff_folder,
            validation_rules=[DirectoryExistsRule],
            files=[],  # Files will be handled dynamically
        )

        # Initialize parent class
        super().__init__(
            pipeline_name="VIRify",
            pipeline_version="2.0",  # TODO: add version to the settings
            directories=[viral_annotation_dir],
        )

    def generate_downloads(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> List[DownloadFile]:
        """
        Generate DownloadFile objects for VIRify results with dynamic file discovery.

        VIRify produces GFF files with variable names, so we need to find them dynamically.

        :param analysis: The analysis object
        :param base_path: Base path containing the pipeline results
        :return: List of DownloadFile objects
        """
        downloads = []
        # Include assembly accession in path (same as validation does)
        assembly_dir = base_path / analysis.assembly.first_accession
        gff_dir = assembly_dir / EMG_CONFIG.virify_pipeline.final_gff_folder

        if not gff_dir.exists():
            return downloads

        # Find all GFF files in the directory (both compressed and uncompressed)
        gff_files = list(gff_dir.glob("*.gff")) + list(gff_dir.glob("*.gff.gz"))

        for gff_file in gff_files:
            download = DownloadFile(
                path=gff_file.relative_to(base_path),
                file_type=DownloadFileType.GFF,
                alias=f"virify_{gff_file.name}",
                download_type=DownloadType.GENOME_ANALYSIS,
                download_group="virify",
                parent_identifier=analysis.accession,
                short_description="Viral annotations",
                long_description="Viral genome annotation from VIRify pipeline",
            )
            downloads.append(download)

        return downloads
