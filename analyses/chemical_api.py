"""Public molecule lookup and assembly molecule search."""

from ninja import Schema
from ninja_extra import api_controller, http_get, http_post
from pydantic import Field

from analyses.models import Analysis, AnalysisMoleculeIndex
from analyses.molecule_search import chebi_candidates, similar_molecules
from emgapiv2.api.schema_utils import ApiSections


class MoleculeQuery(Schema):
    """A SMILES query and an analysis-result cursor."""

    smiles: str = Field(min_length=1, max_length=4000)
    threshold: float = Field(default=0.8, ge=0.4, le=1)
    after: int = Field(default=0, ge=0)


@api_controller("molecule-search", tags=[ApiSections.ANALYSES])
class MoleculeSearchController:
    """Use IDSM matching against public, ready assembly analyses only."""

    @http_get("/compounds", response=list[dict])
    def compounds(self, term: str = ""):
        """Find ChEBI names or identifiers with a usable structure."""
        if not 2 <= len(term.strip()) <= 200:
            return []
        return chebi_candidates(term.strip())

    @http_post("/", response=dict)
    def search(self, payload: MoleculeQuery):
        """Return one page of assembly analyses and their matching molecules."""
        chemistry = similar_molecules(payload.smiles.strip(), payload.threshold)
        scores = {
            int(m["chebi_id"].split(":")[1]): m["score"] for m in chemistry["molecules"]
        }
        visible = Analysis.public_objects.filter(assembly__isnull=False).values("pk")
        hits = list(
            AnalysisMoleculeIndex.objects.filter(
                analysis_id__in=visible,
                chebi_ids__overlap=list(scores),
                analysis_id__gt=payload.after,
            )
            .select_related("analysis", "analysis__assembly")
            .order_by("analysis_id")[:51]
        )
        return {
            **chemistry,
            "items": [
                {
                    "analysis": hit.analysis.accession,
                    "assembly": hit.analysis.assembly.first_accession,
                    "molecules": sorted(
                        [
                            {
                                "chebi_id": f"CHEBI:{identifier}",
                                "score": scores[identifier],
                            }
                            for identifier in hit.chebi_ids
                            if identifier in scores
                        ],
                        key=lambda molecule: (-molecule["score"], molecule["chebi_id"]),
                    ),
                }
                for hit in hits[:50]
            ],
            "next_cursor": hits[49].analysis_id if len(hits) > 50 else None,
        }
