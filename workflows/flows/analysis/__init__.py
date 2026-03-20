"""Analysis workflows."""

from emgapiv2.enum_utils import FutureStrEnum


class AnalysisType(FutureStrEnum):
    """Analysis pipeline types that produce study summaries."""

    AMPLICON = "amplicon"
    RAWREADS = "rawreads"
    ASSEMBLY = "assembly"
