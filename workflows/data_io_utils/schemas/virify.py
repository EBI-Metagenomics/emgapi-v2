from activate_django_first import EMG_CONFIG

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
)
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
            pipeline_version=EMG_CONFIG.virify_pipeline.pipeline_git_revision,
            directories=[viral_annotation_dir],
        )
