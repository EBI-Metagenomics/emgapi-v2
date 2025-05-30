from json import JSONDecodeError
from typing import Union, List, Optional, Literal, Type, Dict, Any

from django.conf import settings
import httpx
from httpx import Auth
from pydantic import BaseModel, Field, model_validator, field_serializer

from workflows.ena_utils.abstract import (
    ENAPortalResultType,
    ENAQuerySetType,
    ENAQueryClause,
    ENAQueryPair,
    _ENAQueryConditions,
    ENAPortalDataPortal,
)
from workflows.ena_utils.read_run import ENAReadRunFields, ENAReadRunQuery
from workflows.ena_utils.sample import ENASampleFields, ENASampleQuery
from workflows.ena_utils.study import ENAStudyFields, ENAStudyQuery
from workflows.ena_utils.analysis import ENAAnalysisFields, ENAAnalysisQuery


EMG_CONFIG = settings.EMG_CONFIG


class ENAAPIRequest(BaseModel):
    result: ENAPortalResultType
    query: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair]
    fields: Union[
        List[ENAStudyFields],
        List[ENAAnalysisFields],
        List[ENASampleFields],
        List[ENAReadRunFields],
    ]
    limit: Optional[int] = Field(None, description="Max number of results to return")
    format: Literal["tsv", "json"] = Field("json")
    data_portal: ENAPortalDataPortal = Field(
        ENAPortalDataPortal.ENA,
        description="The ENA Portal API data portal to query.",
        serialization_alias="dataPortal",
    )

    @model_validator(mode="after")
    def result_and_query_and_return_fields_align(self):
        self._assert_fields_match_result_type()
        self._assert_query_conditions_are_of_type(self.query, self.result)
        return self

    def _assert_fields_match_result_type(self):
        if self.result == ENAPortalResultType.STUDY:
            field_type = ENAStudyFields
        elif self.result == ENAPortalResultType.ANALYSIS:
            field_type = ENAAnalysisFields
        elif self.result == ENAPortalResultType.SAMPLE:
            field_type = ENASampleFields
        elif self.result == ENAPortalResultType.READ_RUN:
            field_type = ENAReadRunFields
        else:
            return

        assert isinstance(self.fields, List)
        assert isinstance(self.fields[0], field_type)

    def _assert_query_conditions_are_of_type(
        self,
        query_part: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair],
        result_type: ENAPortalResultType,
    ):
        if type(query_part) is ENAQuerySetType:
            if result_type == ENAPortalResultType.STUDY:
                assert isinstance(query_part, ENAStudyQuery)
            elif result_type == ENAPortalResultType.ANALYSIS:
                assert isinstance(query_part, ENAAnalysisQuery)
            elif result_type == ENAPortalResultType.SAMPLE:
                assert isinstance(query_part, ENASampleQuery)
            elif result_type == ENAPortalResultType.READ_RUN:
                assert isinstance(query_part, ENAReadRunQuery)
            elif isinstance(query_part, ENAQueryPair):
                self._assert_query_conditions_are_of_type(query_part.left, result_type)
                self._assert_query_conditions_are_of_type(query_part.right, result_type)

    @field_serializer("query")
    def serialize_query(self, query: Type[_ENAQueryConditions], _info):
        return f'"{query}"'

    @field_serializer("fields")
    def serialize_fields(self, fields: Union[List[ENAStudyFields]], _info):
        return ",".join(fields)

    @field_serializer("result")
    def serialize_result_type(self, result: ENAPortalResultType):
        return result.value

    @field_serializer("data_portal")
    def serialize_data_portal(self, result: ENAPortalDataPortal):
        return result.value

    def _parse_response(self, response: httpx.Response, raise_on_empty: bool = True):
        if self.format == "json":
            try:
                j = response.json()
            except JSONDecodeError:
                raise ENAAccessException("Bad JSON response.")
            if isinstance(j, dict) and "message" in j:
                raise ENAAccessException(f"Error response: {j['message']}")
            elif isinstance(j, list) and len(j) == 0 and raise_on_empty:
                raise ENAAvailabilityException("Empty response.")
            return j
        return response.text  # TODO: tsv

    def get(
        self, auth: Type[Auth] = None, raise_on_empty: bool = True
    ) -> Union[List[Dict[str, Any]], str]:
        url = EMG_CONFIG.ena.portal_search_api
        params = self.model_dump(by_alias=True)
        r = httpx.get(
            url=url,
            params=params,
            auth=auth,
        )
        if httpx.codes.is_error(r.status_code):
            raise ENAAccessException(r.text)
        return self._parse_response(r, raise_on_empty=raise_on_empty)


class ENAAccessException(Exception): ...


class ENAAvailabilityException(Exception): ...
