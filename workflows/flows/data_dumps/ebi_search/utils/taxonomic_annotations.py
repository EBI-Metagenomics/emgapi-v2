import re

from analyses.models import Analysis


def taxonomy_lineages(analysis: Analysis) -> list[list[str]]:
    taxonomies = analysis.annotations.get(Analysis.TAXONOMIES, {})
    groups = taxonomies.values() if isinstance(taxonomies, dict) else [taxonomies]
    lineages: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    for group in groups:
        for annotation in group or []:
            organism = (
                annotation.get("organism", "")
                if isinstance(annotation, dict)
                else str(annotation)
            )
            organism = organism.split("|", 1)[0]
            separator = ";" if ";" in organism else ":"
            lineage = [
                re.sub(r"^[A-Za-z]+__", "", part.strip())
                for part in organism.split(separator)
                if part.strip()
            ]
            lineage = [part for part in lineage if part]
            lineage_key = tuple(lineage)
            if len(lineage) > 1 and lineage_key not in seen:
                seen.add(lineage_key)
                lineages.append(lineage)
    return lineages
