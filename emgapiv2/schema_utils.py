from emgapiv2.enum_utils import FutureStrEnum


class ApiSections(FutureStrEnum):
    STUDIES = "Studies"
    SAMPLES = "Samples"
    ANALYSES = "Analyses"
    REQUESTS = "Requests"


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
) -> dict:
    return {
        f"Get{related_object_name.capitalize()}For{self_object_name.capitalize()}": {
            OpenApiKeywords.OPERATIONID.value: related_detail_operation_id,
            OpenApiKeywords.PARAMETERS.value: {
                related_lookup_param: f"$response.body#/{related_id_in_response}"
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
