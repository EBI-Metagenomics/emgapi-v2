import analyses.models
from activate_django_first import EMG_CONFIG
from analyses.base_models.with_downloads_models import (
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
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
        super().__init__(
            pipeline_name="mobilome-annotation-pipeline",
            pipeline_version=EMG_CONFIG.map_pipeline.pipeline_git_revision,
            directories=[mobilome_annotation_dir],
        )
