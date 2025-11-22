from __future__ import annotations

from typing import Optional

from django.db.models import Q
from ninja import FilterSchema
from pydantic import Field

from analyses.models import Biome
from emgapiv2.enum_utils import FutureStrEnum


class ApiSections(FutureStrEnum):
    STUDIES = "Studies"
    SAMPLES = "Samples"
    ANALYSES = "Analyses"
    REQUESTS = "Requests"
    PRIVATE_DATA = "Private Data"
    GENOMES = "Genomes"
    ASSEMBLIES = "Assemblies"


class OpenApiKeywords(FutureStrEnum):
    NAME = "name"
    DESCRIPTION = "description"
    RESPONSES = "responses"
    LINKS = "links"
    OPERATIONID = "operationId"
    PARAMETERS = "parameters"


def make_related_detail_link(
    related_detail_operation_id: str,
    related_object_name: str,
    self_object_name: str,
    related_id_in_response: str,
    related_lookup_param: str = "accession",
    from_list_to_detail: bool = False,
    from_list_at_path: str = "items/",
) -> dict:
    if from_list_to_detail:
        link_name = f"Get{related_object_name.capitalize()}From{self_object_name.capitalize()}List"
    else:
        link_name = (
            f"Get{related_object_name.capitalize()}For{self_object_name.capitalize()}"
        )
    from_list_at_path = from_list_at_path.rstrip("/") + "/"
    return {
        link_name: {
            OpenApiKeywords.OPERATIONID.value: related_detail_operation_id,
            OpenApiKeywords.PARAMETERS.value: {
                related_lookup_param: f"$response.body#/{from_list_at_path if from_list_to_detail else ''}{'0/' if from_list_to_detail else ''}{related_id_in_response}"
            },
            OpenApiKeywords.DESCRIPTION.value: f"The {related_id_in_response} is an identifier that can be used to access the {related_object_name} detail",
        }
    }


def make_links_section(links: dict, response_code: int = 200) -> dict:
    return {
        OpenApiKeywords.RESPONSES.value: {
            response_code: {OpenApiKeywords.LINKS.value: links}
        }
    }

def make_child_link(
    operation_id: str,
    child_name: str,
    self_object_name: str,
    path_param: str = "accession",
    description: str | None = None,
) -> dict:
    link_name = f"Get{child_name.capitalize().replace('-', '')}For{self_object_name.capitalize()}"
    return {
        link_name: {
            "operationId": operation_id,
            "parameters": {path_param: "$request.path.accession"},
            **({"description": description} if description else {}),
        }
    }

class BiomeFilter(FilterSchema):
    biome_lineage: Optional[str] = Field(
        None, description="The lineage to match, including all descendant biomes"
    )

    def filter_biome_lineage(self, lineage: str | None) -> Q:
        if not lineage:
            return Q()
        return Q(biome__path__descendants=Biome.lineage_to_path(lineage))
