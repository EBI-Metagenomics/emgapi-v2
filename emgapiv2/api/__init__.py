from textwrap import dedent

from ninja_extra import NinjaExtraAPI

from emgapiv2.api.schema_utils import OpenApiKeywords, ApiSections
from .analyses import AnalysisController
from .private import MyDataController
from .samples import SampleController
from .studies import StudyController
from .token_controller import WebinJwtController

api = NinjaExtraAPI(
    title="MGnify API",
    description="The API for [MGnify](https://www.ebi.ac.uk/metagenomics), "
    "EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.",
    urls_namespace="api",
    version="2.0-alpha",
    docs_url="/",
    openapi_extra={
        "tags": [
            {
                OpenApiKeywords.NAME: ApiSections.STUDIES,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    MGnify studies are based on ENA studies/projects, and are collections of samples, runs, assemblies,
                    and analyses associated with a certain set of experiments.
                    """
                ),
            },
            {
                OpenApiKeywords.NAME: ApiSections.SAMPLES,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    MGnify samples are based on ENA/BioSamples samples, and represent individual biological samples.
                    """
                ),
            },
            {
                OpenApiKeywords.NAME: ApiSections.ANALYSES,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    MGnify analyses are runs of a standard pipeline on an individual sequencing run or assembly.
                    They can include collections of taxonomic and functional annotations.
                    """
                ),
            },
            {
                OpenApiKeywords.NAME: ApiSections.PRIVATE_DATA,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    MGnify supports private data, inheriting ENA's data privacy model and public release times.
                    Authentication is required to view private data owned by a Webin account.
                    """
                ),
            },
            {
                OpenApiKeywords.NAME: ApiSections.AUTH,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    A Token can be obtained using an ENA Webin username and password,
                    to access the Private Data endpoints.
                    """
                ),
            },
            # {
            #     OpenApiKeywords.NAME: ApiSections.REQUESTS,
            #     OpenApiKeywords.DESCRIPTION: dedent(
            #         """
            #         Requests are user-initiated processes for MGnify to assemble and/or analyse the samples in a study.
            #         """
            #     ),
            # },
        ]
    },
)

api.register_controllers(AnalysisController)
api.register_controllers(SampleController)
api.register_controllers(StudyController)
api.register_controllers(MyDataController)

# Private data auth token provider (Webin JWTs)
api.register_controllers(WebinJwtController)
