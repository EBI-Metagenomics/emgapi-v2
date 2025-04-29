from datetime import date
from enum import Enum
from typing import Union, TypeVar

from pydantic import BaseModel, Field, computed_field

# Define TypeVars for self-referential types
T_ENAQueryClause = TypeVar("T_ENAQueryClause", bound="ENAQueryClause")
T_ENAQueryPair = TypeVar("T_ENAQueryPair", bound="ENAQueryPair")
T_ENAQueryConditions = TypeVar("T_ENAQueryConditions", bound="_ENAQueryConditions")


class ENAPortalResultType(str, Enum):
    ANALYSIS = "analysis"  # Nucelotide sequence analyses from reads
    ANALYSIS_STUDY = (
        "analysis_study"  # Studies used for nucleotide sequence analyses from reads
    )
    ASSEMBLY = "assembly"  # Genome assemblies
    CODING = "coding"  # Coding sequences
    NONCODING = "noncoding"  # Non-coding sequences
    READ_EXPERIMENT = "read_experiment"  # Experiments used for raw reads
    READ_RUN = "read_run"  # Raw reads
    READ_STUDY = "read_study"  # Studies used for raw reads
    SAMPLE = "sample"
    STUDY = "study"
    TAXON = "taxon"  # Taxonomic classification
    TLS_SET = "tls_set"  # Targeted locus study contig sets (TLS)
    TSA_SET = "tsa_set"  # Transcriptome assembly contig sets (TSA)
    WGS_SET = "wgs_set"  # Genome assembly contig set (WGS)


class ENAPortalDataPortal(str, Enum):
    ENA = "ena"
    METAGENOME = "metagenome"
    FAANG = "faang"
    PATHOGEN = "pathogen"


class ENAQueryOperators(str, Enum):
    OR = "OR"
    AND = "AND"
    NOT = "NOT"


class ENAQueryClause(BaseModel):
    search_field: str
    value: Union[str, int, date]
    is_not: bool = Field(default=False)

    def __str__(self):
        value = self.value
        if isinstance(value, date):
            value = value.strftime("%Y-%m-%d")
        return f"{ENAQueryOperators.NOT if self.is_not else ''} {self.search_field}={value}".strip()

    def __or__(self, other: Union[T_ENAQueryClause, "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: Union[T_ENAQueryClause, "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> T_ENAQueryClause:
        return ENAQueryClause(
            search_field=self.search_field, value=self.value, is_not=not self.is_not
        )


ENAQuerySetType = TypeVar("ENAQuerySetType", bound="_ENAQueryConditions")


class ENAQueryPair(BaseModel):
    operator: ENAQueryOperators = Field(ENAQueryOperators.AND)
    left: Union[ENAQueryClause, T_ENAQueryPair, ENAQuerySetType]
    right: Union[ENAQueryClause, T_ENAQueryPair, ENAQuerySetType]
    is_not: bool = Field(default=False)

    def __str__(self):
        return f"{ENAQueryOperators.NOT + ' ' if self.is_not else ''}({str(self.left)} {self.operator.value} {str(self.right)})"

    def __or__(
        self, other: Union[ENAQueryClause, T_ENAQueryPair, ENAQuerySetType]
    ) -> T_ENAQueryPair:
        return self.__class__(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(
        self, other: Union[ENAQueryClause, T_ENAQueryPair, ENAQuerySetType]
    ) -> T_ENAQueryPair:
        return self.__class__(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> T_ENAQueryPair:
        return self.__class__(
            left=self.left,
            operator=self.operator,
            right=self.right,
            is_not=not self.is_not,
        )


class _ENAQueryConditions(BaseModel):
    is_not: bool = Field(default=False)

    # Define specific fields on inheriting models e.g.:
    # class ENAStudyQuery(ENAQueryConditions):
    #     description: Optional[str] = Field(None, description="brief sequence description")

    @computed_field
    @property
    def queries(self) -> Union[ENAQueryPair, ENAQueryClause]:
        # combine all set fields with an AND.
        # technically this makes nested pairs, which is not optimal, but is reasonable for most use cases.
        # e.g. ((study_description=hello AND tax_id=1) AND broker_name=EMG)
        # could be simplified to (study_description=hello AND tax_id=1 AND broker_name=EMG)
        clauses = None
        for search_field, value in self.model_dump(
            exclude={"is_not", "queries"}
        ).items():
            if value is None:
                continue
            if clauses:
                clauses &= ENAQueryClause(search_field=search_field, value=value)
            else:
                clauses = ENAQueryClause(search_field=search_field, value=value)

        return clauses

    def __str__(self):
        return str(self.queries)

    def __or__(self, other: ENAQuerySetType) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: ENAQuerySetType) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> T_ENAQueryConditions:
        already_set = self.model_dump(exclude={"is_not"})
        return self.__class__(**already_set, is_not=not self.is_not)
