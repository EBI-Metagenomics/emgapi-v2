"""External chemical lookup for the minimal molecule search."""

import json
import re

import httpx
from django.core.cache import cache
from ninja.errors import HttpError

CHEBI_API = "https://www.ebi.ac.uk/chebi/backend/api/public/"
IDSM = "https://idsm.elixir-czech.cz/sparql/endpoint/chebi"
MAX_MOLECULES = 100


def chemical_json(url: str, params: dict) -> dict:
    """Read a fixed chemical provider endpoint with a bounded timeout."""
    try:
        response = httpx.get(url, params=params, timeout=30, follow_redirects=True)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HttpError(
            502, "Chemical lookup is unavailable. Please try again."
        ) from exc


def chebi_candidates(term: str) -> list[dict]:
    """Return named ChEBI candidates with structures."""
    data = chemical_json(CHEBI_API + "es_search/", {"term": term, "size": 10})
    try:
        return [
            {
                "chebi_id": source["chebi_accession"],
                "name": source["ascii_name"],
                "smiles": source["smiles"],
            }
            for hit in data["results"]
            if (source := hit["_source"]).get("smiles")
        ]
    except (KeyError, TypeError) as exc:
        raise HttpError(502, "ChEBI returned an invalid response.") from exc


def similar_molecules(smiles: str, threshold: float) -> dict:
    """Search ChEBI structures through IDSM; cache only public chemistry."""
    import hashlib

    key = "chem-search:" + hashlib.sha256(f"{smiles}:{threshold}".encode()).hexdigest()
    cached = cache.get(key)
    if cached is not None:
        return cached
    query = f"""
PREFIX sachem: <http://bioinfo.uochb.cas.cz/rdf/v1.0/sachem#>
SELECT ?compound ?score WHERE {{
  [ sachem:compound ?compound; sachem:score ?score ] sachem:similaritySearch [
    sachem:query {json.dumps(smiles)};
    sachem:cutoff {threshold}; sachem:topn {MAX_MOLECULES + 1}
  ].
}} ORDER BY DESC(?score) ?compound
"""
    data = chemical_json(
        IDSM, {"query": query, "format": "application/sparql-results+json"}
    )
    try:
        matches = []
        for row in data["results"]["bindings"]:
            identifier = re.search(r"CHEBI[_:](\d+)$", row["compound"]["value"])
            if not identifier:
                raise ValueError("Unexpected compound identifier")
            matches.append(
                {
                    "chebi_id": "CHEBI:" + identifier[1],
                    "score": float(row["score"]["value"]),
                }
            )
        result = {
            "molecules": matches[:MAX_MOLECULES],
            "truncated": len(matches) > MAX_MOLECULES,
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise HttpError(502, "IDSM returned an invalid response.") from exc
    cache.set(key, result, 3600)
    return result
