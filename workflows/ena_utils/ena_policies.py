from emgapiv2.enum_utils import FutureStrEnum

ALLOWED_LIBRARY_SOURCE: list = ["METAGENOMIC", "METATRANSCRIPTOMIC"]
COMMONLY_INCORRECT_LIBRARY_SOURCE: list = ["GENOMIC", "OTHER"]
SINGLE_END_LIBRARY_LAYOUT: str = "SINGLE"
PAIRED_END_LIBRARY_LAYOUT: str = "PAIRED"
METAGENOME_SCIENTIFIC_NAME: str = "metagenome"


class ENALibraryStrategyPolicy(FutureStrEnum):
    """
    Each policy determines a trust vs. override level for the library strategy metadata in ENA.
    """

    ONLY_IF_CORRECT_IN_ENA = "only_if_correct_in_ena"
    ASSUME_OTHER_ALSO_MATCHES = "assume_other_also_matches"
    OVERRIDE_ALL = "override_all"


class ENALibrarySourcePolicy(FutureStrEnum):
    """
    Each policy determines how to handle the library source metadata in ENA.
    """

    ONLY_IF_METAGENOMIC_IN_ENA = "only_if_metagenomic_in_ena"
    OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME = (
        "override_genomic_if_metagenomic_scientific_name"
    )
    OVERRIDE_ALL_TO_METAGENOMIC = "override_all_to_metagenomic"
