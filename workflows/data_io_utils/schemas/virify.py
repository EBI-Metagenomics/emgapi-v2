from activate_django_first import EMG_CONFIG
import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFileMetadata,
    DownloadFileType,
    DownloadType,
)

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from .base import (
    PipelineDirectorySchema,
    PipelineResultSchema,
    PipelineFileSchema,
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
            files=[
                PipelineFileSchema(
                    filename_template="{identifier}_virify.gff",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=analyses.models.Analysis.VIRIFY,
                        short_description="Viral annotations",
                        long_description="VIRify viral annotation in GFF format",
                    ),
                ),
            ],
        )

        # Initialize parent class
        super().__init__(
            pipeline_name="VIRify",
            pipeline_version=EMG_CONFIG.virify_pipeline.pipeline_git_revision,
            directories=[viral_annotation_dir],
        )
