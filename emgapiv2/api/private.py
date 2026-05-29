import logging
import re
from typing import Literal

from ninja import Schema
from ninja.errors import HttpError
from ninja_extra import ControllerBase, api_controller, http_get, http_post, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

import analyses.models
from analyses.schemas import (
    MGnifyStudy,
)
from emgapiv2.api import ApiSections
from emgapiv2.api.auth import (
    DjangoSuperUserAuth,
    WebinJWTAuth,
    get_webin_account_details_via_broker,
)
from emgapiv2.rt import rt_client, rt_config
from workflows.ena_utils.ena_accession_matching import (
    INSDC_PROJECT_ACCESSION_REGEX,
    INSDC_STUDY_ACCESSION_REGEX,
)

logger = logging.getLogger(__name__)


class AnalysisRequest(Schema):
    study_accession: str
    analysis_type: Literal["Analysis", "Assembly+Analysis"]
    request_type: Literal["Public", "Private"]
    comments: str = Field(None)


class RequestSuccessResponse(Schema):
    message: str


@api_controller("my-data", tags=[ApiSections.PRIVATE_DATA])
class MyDataController(ControllerBase):
    @http_get(
        "/studies/",
        response=NinjaPaginationResponseSchema[MGnifyStudy],
        tags=[ApiSections.PRIVATE_DATA],
        summary="List all private studies available from MGnify",
        operation_id="list_private_mgnify_studies",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth()],
    )
    @paginate()
    def list_private_mgnify_studies(self):
        auth = self.context.request.auth
        qs = analyses.models.Study.objects

        if auth and auth.is_superuser:
            qs = qs.all()
        elif auth and (webin_id := auth.username):
            qs = qs.filter(webin_submitter=webin_id)
        else:
            qs = qs.none()

        return qs

    @http_post(
        "/request",
        tags=[ApiSections.PRIVATE_DATA],
        summary="Request analysis of a study",
        operation_id="request_analysis",
        auth=[WebinJWTAuth()],
        description="Request an analysis of a study. This is an internal API endpoint to support the MGnify website, not intended for programmatic use by end users.",
        include_in_schema=False,
        response=RequestSuccessResponse,
    )
    def request_analysis(self, request_detail: AnalysisRequest):
        study_accession = request_detail.study_accession
        analysis_type = request_detail.analysis_type
        request_type = request_detail.request_type
        comments = request_detail.comments

        auth = self.context.request.auth
        if not auth:
            logger.warning("Attempt to request analysis without authentication.")
            raise HttpError(401, "You must be logged in to request an analysis.")
        elif webin_id := auth.username:
            # user is authenticated, try to get their account details via broker
            account_details = get_webin_account_details_via_broker(webin_id)
            if not account_details:
                logger.error(f"Failed to fetch account details for {webin_id}")
                raise HttpError(
                    401,
                    "Webin account is not accessible, have you consented to metagenome analysis and set up contact details in the Webin portal?",
                )
        else:
            logger.warning("Attempt to request analysis with wrong authentication.")
            raise HttpError(
                401,
                "You must be logged in with a Webin account to request an analysis.",
            )

        mgys_accession_regex: re.Pattern = re.compile("(MGYS[0-9]{8,10})")

        if not (
            INSDC_STUDY_ACCESSION_REGEX.fullmatch(study_accession)
            or INSDC_PROJECT_ACCESSION_REGEX.fullmatch(study_accession)
            or mgys_accession_regex.fullmatch(study_accession)
        ):
            raise HttpError(400, "Invalid study accession.")

        rt_message = (
            f"Study accession: {study_accession};"
            f"{request_type};"
            f"Analysis type: {analysis_type};"
            f"Requester name: {account_details[0].requester_name};"
            f"Email: {account_details[0].email_address}"
            f"Webin: {webin_id};"
            f"Additional notes: {comments};"
        )

        emails = ",".join([contact.email_address for contact in account_details])

        try:
            rt_ticket = rt_client.create_ticket(
                queue=rt_config.rt_requests_queue,
                subject=f"{request_type} {analysis_type} request: {study_accession}",
                content=rt_message,
                requestor=emails,
            )
        except BaseException as e:
            logger.error(f"Failed to create RT ticket: {e}")
            raise HttpError(424, "Failed to submit request. Please try again later.")

        logger.info(f"Ticket {rt_ticket} created for {rt_message} by {emails}")
        return {
            "message": f"Request submitted successfully, with ticker number {rt_ticket}."
        }
