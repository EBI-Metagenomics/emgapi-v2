import re

_INSDC_CENTRE_PREFIXES = "EDS"

INSDC_STUDY_ACCESSION_REGEX: str = f"([{_INSDC_CENTRE_PREFIXES}]RP[0-9]{{6,}})"
INSDC_STUDY_ACCESSION_GLOB: str = f"[{_INSDC_CENTRE_PREFIXES}]RP[0-9]*"

INSDC_PROJECT_ACCESSION_REGEX: str = "(PRJ[NED][AB][0-9]+)"  # PRJNA, PRJEB, PRJDB
INSDC_PROJECT_ACCESSION_GLOB: str = "PRJ[NED][AB][0-9]*"

ENA_ASSEMBLY_ACCESSION_REGEX: str = f"([{_INSDC_CENTRE_PREFIXES}]RZ[0-9]{{6,}})"
ENA_ASSEMBLY_ACCESSION_GLOB: str = f"[{_INSDC_CENTRE_PREFIXES}]RZ[0-9]*"


def extract_all_accessions(accessions_from_api: str | list[str]) -> list[str]:
    all_accessions = set()
    for accession in (
        accessions_from_api
        if isinstance(accessions_from_api, list)
        else [accessions_from_api]
    ):
        for acc in accession.split(";"):
            if acc:
                all_accessions.add(acc)
    return sorted(all_accessions)


def extract_study_accession_from_study_title(study_title: str) -> str | None:
    matches = re.findall(INSDC_PROJECT_ACCESSION_REGEX, study_title)
    matches.extend(re.findall(INSDC_STUDY_ACCESSION_REGEX, study_title))
    if len(matches) == 1:
        return matches[0]
    else:
        return None
