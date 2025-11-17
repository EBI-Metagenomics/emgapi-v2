import pytest

from analyses.base_models.with_downloads_models import (
    DownloadFileMetadata,
    DownloadFileType,
    DownloadType,
    DownloadFileIndexFileMetadata,
    DownloadFile,
)
from analyses.models import Analysis
from workflows.data_io_utils.file_rules.common_rules import FileExistsRule
from workflows.data_io_utils.schemas import PipelineFileSchema


@pytest.mark.django_db
def test_pipeline_file_schema_conversion_to_download_file(assembly_analysis, tmp_path):
    mgya: Analysis = Analysis.objects.first()
    erz = mgya.assembly.first_accession
    gff = tmp_path / f"{erz}_data.gff.gz"

    schema = PipelineFileSchema(
        filename_template="{identifier}_data.gff.gz",
        validation_rules=[FileExistsRule],
        download_metadata=DownloadFileMetadata(
            file_type=DownloadFileType.GFF,
            download_type=DownloadType.FUNCTIONAL_ANALYSIS,
            download_group="annotation_summary",
            short_description="Annotation summary GFF",
            long_description="Comprehensive annotation summary in GFF format",
        ),
        index_files=[
            DownloadFileIndexFileMetadata(index_type="gzi"),
            DownloadFileIndexFileMetadata(index_type="csi"),
        ],
    )

    # should fail if file doesn't exist
    with pytest.raises(FileNotFoundError) as exc_info:
        DownloadFile.from_pipeline_file_schema(
            schema,
            parent_object=mgya,
            directory_path=tmp_path,
            base_path=tmp_path,
            file_identifier_lookup_string="assembly.first_accession",
        )
    assert str(exc_info.value) == f"Required file {gff} not found"

    gff.touch()

    gzi = tmp_path / f"{erz}_data.gff.gz.gzi"
    gzi.touch()
    csi = tmp_path / f"{erz}_data.gff.gz.csi"

    # should fail if an index file doesn't exist
    with pytest.raises(FileNotFoundError) as exc_info:
        DownloadFile.from_pipeline_file_schema(
            schema,
            parent_object=mgya,
            directory_path=tmp_path,
            base_path=tmp_path,
            file_identifier_lookup_string="assembly.first_accession",
        )
    assert str(exc_info.value) == f"Required index file {csi} not found"

    # should succeed if all files exist
    csi.touch()

    download_file = DownloadFile.from_pipeline_file_schema(
        schema,
        parent_object=mgya,
        directory_path=tmp_path,
        base_path=tmp_path,
        file_identifier_lookup_string="assembly.first_accession",
    )
    assert str(download_file.path) == str(gff.relative_to(tmp_path))
    assert str(download_file.index_file[0].path) == str(gzi.relative_to(tmp_path))
    assert str(download_file.index_file[1].path) == str(csi.relative_to(tmp_path))
    assert download_file.file_type == DownloadFileType.GFF
    assert download_file.download_type == DownloadType.FUNCTIONAL_ANALYSIS
    assert download_file.download_group == "annotation_summary"
    assert download_file.short_description == "Annotation summary GFF"
    assert (
        download_file.long_description
        == "Comprehensive annotation summary in GFF format"
    )
