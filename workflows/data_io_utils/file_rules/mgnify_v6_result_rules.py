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
    taxid: Optional[int] = Field(None)

    model_config = ConfigDict(extra="allow")  # e.g. for SSU, PR2 etc columns


FileConformsToTaxonomyTSVSchemaRule = generate_csv_schema_file_rule(
    TaxonomyTSVRow, delimiter=CSVDelimiter.TAB, none_values=[""]
)


class RawReadsTaxonomyTSVRow(BaseModel):
    read_count: Union[float, int] = Field(alias="Count")
    sk_taxonomy: Optional[str] = Field(default=None, alias="Superkingdom")
    k_taxonomy: Optional[str] = Field(default=None, alias="Kingdom")
    p_taxonomy: Optional[str] = Field(default=None, alias="Phylum")
    c_taxonomy: Optional[str] = Field(default=None, alias="Class")
    o_taxonomy: Optional[str] = Field(default=None, alias="Order")
    f_taxonomy: Optional[str] = Field(default=None, alias="Family")
    g_taxonomy: Optional[str] = Field(default=None, alias="Genus")
    s_taxonomy: Optional[str] = Field(default=None, alias="Species")


FileConformsToRawReadsTaxonomyTSVSchemaRule = generate_csv_schema_file_rule(
    RawReadsTaxonomyTSVRow, delimiter=CSVDelimiter.TAB, none_values=[""]
)


class FunctionalTSVRow(BaseModel):
    function: str
    read_count: Union[float, int]
    coverage_depth: float
    coverage_breadth: float

    model_config = ConfigDict(extra="allow")


FileConformsToFunctionalTSVSchemaRule = generate_csv_schema_file_rule(
    FunctionalTSVRow, delimiter=CSVDelimiter.TAB, none_values=[""]
)

GlobOfTaxonomyFolderHasHtmlAndMseqRule = GlobRule(
    rule_name="Folder should contain html and mseq files",
    glob_patten="*",
    test=lambda files: sum(f.suffix in [".html", ".mseq"] for f in files) == 2,
)

GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule = GlobRule(
    rule_name="Folder should contain html and krona txt files",
    glob_patten="**",
    test=lambda files: sum(f.suffix in [".html", ".txt"] for f in files) == 2,
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

GlobOfMultiqcFolderHasMultiqc = GlobRule(
    rule_name="Folder should contain multiQC report file",
    glob_patten="*",
    test=lambda files: sum(f.name.endswith("multiqc_report.html") for f in files) == 1,
)

GlobOfQcFolderHasFastp = GlobRule(
    rule_name="Folder should contain fastp file",
    glob_patten="*/*",
    test=lambda files: sum(f.name.endswith("fastp.json") for f in files) == 1,
)

GlobOfDecontamFolderHasSummaryStats = GlobRule(
    rule_name="Folder should contain at least one summary stats file",
    glob_patten="*/*",
    test=lambda files: sum(f.name.endswith("summary_stats.txt") for f in files) > 0,
)

GlobOfAsvFolderHasRegionFolders = GlobRule(
    rule_name="Folder should contain either one region subfolder, or two regions plus a concatenation, with asv-read-count files",
    glob_patten="*/*_asv_read_counts.tsv",
    test=lambda files: len(list(files))
    in [1, 3],  # e.g. ["16S-V3-V4"] or ["18S-V9", "16S-V3-V4", "concat"]
)

GlobOfFolderHasTsvGzAndIndex = GlobRule(
    rule_name="Folder should contain a .tsv.gz file and sibling .tsv.gz.gzi",
    glob_patten="*.tsv.gz*",
    test=lambda files: sum(f.suffix in [".gz", ".gzi"] for f in files) % 2
    == 0,  # in case e.g. 2 sets of tsv.gz files
)
