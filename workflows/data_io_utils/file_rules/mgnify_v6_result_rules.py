from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field, AliasChoices

from workflows.data_io_utils.csv.csv_comment_handler import CSVDelimiter
from workflows.data_io_utils.file_rules.base_rules import GlobRule
from workflows.data_io_utils.file_rules.rule_factories import (
    generate_csv_schema_file_rule,
)


class TaxonomyTSVRow(BaseModel):
    otu_id: int = Field(..., alias="OTU ID")
    read_count: Union[float, int] = Field(
        ..., validation_alias=AliasChoices("SSU", "PR2", "ITSonedb", "UNITE", "LSU")
    )
    taxonomy: str
    taxid: Optional[int]

    model_config = ConfigDict(extra="allow")  # e.g. for SSU, PR2 etc columns


FileConformsToTaxonomyTSVSchemaRule = generate_csv_schema_file_rule(
    TaxonomyTSVRow, delimiter=CSVDelimiter.TAB
)

GlobOfTaxonomyFolderHasHtmlAndMseqRule = GlobRule(
    rule_name="Folder should contain html and mseq files",
    glob_patten="*",
    test=lambda files: sum(f.suffix in [".html", ".mseq"] for f in files) == 2,
)

GlobOfQcFolderHasFastpAndMultiqc = GlobRule(
    rule_name="Folder should contain fastp and multiqc files",
    glob_patten="*",
    test=lambda files: sum(
        f.name.endswith("multiqc_report.html") or f.name.endswith("fastp.json")
        for f in files
    )
    == 2,
)

GlobOfAsvFolderHasRegionFolders = GlobRule(
    rule_name="Folder should contain either one region subfolder, or two regions plus a concatenation, with asv-read-count files",
    glob_patten="*/*_asv_read_counts.tsv",
    test=lambda files: len(list(files))
    in [1, 3],  # e.g. ["16S-V3-V4"] or ["18S-V9", "16S-V3-V4", "concat"]
)
