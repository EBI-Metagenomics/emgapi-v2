import pytest
from pydantic import ValidationError

from workflows.data_io_utils.file_rules.base_rules import FileRule, GlobRule
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
    GlobHasFilesCountRule,
    GlobHasFilesRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    FileConformsToTaxonomyTSVSchemaRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File


def test_file_rules_utils(tmp_path):
    # no rules applied, no error should be raised
    f = File(
        path=tmp_path / "non-exist.tsv",
    )
    assert f.path == tmp_path / "non-exist.tsv"

    # apply existence rule
    with pytest.raises(ValidationError):
        File(path=tmp_path / "non-exist.tsv", rules=[FileExistsRule])

    # empty file should be okay
    (tmp_path / "empty.tsv").touch()
    File(path=tmp_path / "empty.tsv", rules=[FileExistsRule])

    # apply size rule
    with pytest.raises(ValidationError):
        File(path=tmp_path / "empty.tsv", rules=[FileExistsRule, FileIsNotEmptyRule])

    # new custom rule to check content
    MyRule = FileRule(
        rule_name="Text contains hello", test=lambda p: "hello" in p.read_text()
    )

    with pytest.raises(ValidationError) as exc_info:
        File(path=tmp_path / "empty.tsv", rules=[MyRule])
        assert "Text contains hello" in exc_info.value

    hello_file = tmp_path / "hello.tsv"
    hello_file.write_text("hello world")
    File(path=hello_file, rules=[MyRule])


def test_dir_rules_utils(tmp_path):
    # non exist directory with no rules is fine
    d = Directory(
        path=tmp_path / "non-exist",
    )
    assert d.path == tmp_path / "non-exist"

    with pytest.raises(ValidationError):
        d = Directory(path=tmp_path / "non-exist", rules=[DirectoryExistsRule])

    # empty dir fine if it exists
    empty = tmp_path / "empty"
    empty.mkdir(exist_ok=True, parents=True)

    Directory(path=empty, rules=[DirectoryExistsRule])

    # but should fail glob test if no files
    with pytest.raises(ValidationError):
        Directory(
            path=empty, rules=[DirectoryExistsRule], glob_rules=[GlobHasFilesRule]
        )

    # adding a file should fix it
    (empty / "something.txt").touch()

    Directory(path=empty, rules=[DirectoryExistsRule], glob_rules=[GlobHasFilesRule])

    # test custom glob test
    ShouldContainHelloRule = GlobRule(
        rule_name="Directory contains a file matching hello",
        glob_patten="*hello*",
        test=lambda matches: len(list(matches)) > 0,
    )

    with pytest.raises(ValidationError) as exc_info:
        Directory(path=empty, glob_rules=[ShouldContainHelloRule])
        assert "Directory contains a file matching hello" in exc_info.value

    (empty / "hello.txt").touch()
    Directory(path=empty, glob_rules=[ShouldContainHelloRule])


def test_csv_schema_rule_utils(tmp_path):
    tsv_file = tmp_path / "data.tsv"
    content = """\
# Constructed from biom file
# OTU ID	SSU	taxonomy	taxid
36901	1.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli	91061
60237	2.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Carnobacteriaceae;g__Trichococcus	82802\
    """
    with tsv_file.open("w", newline="") as fh:
        fh.write(content)

    # should adhere to MGnify V6 Taxonomy schema
    File(path=tsv_file, rules=[FileConformsToTaxonomyTSVSchemaRule])

    # should work with no leading comment line
    content = """\
# OTU ID	SSU	taxonomy	taxid
36901	1.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli	91061
60237	2.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Carnobacteriaceae;g__Trichococcus	82802\
    """
    with tsv_file.open("w", newline="") as fh:
        fh.write(content)

    File(path=tsv_file, rules=[FileConformsToTaxonomyTSVSchemaRule])

    # should work with no # comment character on header
    content = """\
OTU ID	SSU	taxonomy	taxid
36901	1.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli	91061
60237	2.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Carnobacteriaceae;g__Trichococcus	82802\
    """
    with tsv_file.open("w", newline="") as fh:
        fh.write(content)

    File(path=tsv_file, rules=[FileConformsToTaxonomyTSVSchemaRule])

    # should fail if missing taxonomy column
    content = """\
OTU ID	SSU	taxon	taxid
36901	1.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli	91061
60237	2.0	sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Carnobacteriaceae;g__Trichococcus	82802\
    """
    with tsv_file.open("w", newline="") as fh:
        fh.write(content)
    with pytest.raises(ValidationError) as exc_info:
        File(path=tsv_file, rules=[FileConformsToTaxonomyTSVSchemaRule])
        assert "taxonomy" in exc_info.value


def test_glob_file_count_rules(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir(exist_ok=True, parents=True)

    # should fail if we expect empty dir to have 1 file
    with pytest.raises(ValidationError) as exc_info:
        Directory(path=empty, glob_rules=[GlobHasFilesCountRule[1]])
        assert "Glob should have 1 file(s)" in exc_info.value

    # should pass if we expect it to have 0
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[0]])

    # should pass if we expect it to have 0 or more
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[0:]])

    # should pass if we expect it to have 0 to 2
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[0:2]])

    (empty / "something.txt").touch()

    # should pass if we expect it to have 1 or more
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[1:]])

    # ... or exactly 1
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[1]])

    # ... or 1 to 10
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[1:10]])

    # ... or up to 10
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[:10]])

    # ... or up to 1
    Directory(path=empty, glob_rules=[GlobHasFilesCountRule[:1]])

    (empty / "something-else.txt").touch()

    # should fail if max 1 is allowed
    with pytest.raises(ValidationError) as exc_info:
        Directory(path=empty, glob_rules=[GlobHasFilesCountRule[:1]])
        assert "Glob should have :2 file(s)" in exc_info.value
