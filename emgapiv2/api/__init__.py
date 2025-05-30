from textwrap import dedent

from ninja import NinjaAPI
from ninja.pagination import RouterPaginated

from emgapiv2.api.schema_utils import OpenApiKeywords, ApiSections
from .analyses import router as analyses_router
from .private import router as my_data_router
from .samples import router as samples_router
from .studies import router as studies_router

api = NinjaAPI(
    title="MGnify API",
    description="The API for [MGnify](https://www.ebi.ac.uk/metagenomics), "
    "EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.",
    urls_namespace="api",
    csrf=True,
    version="2.0-alpha",
    default_router=RouterPaginated(),
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

api.add_router("/analyses", analyses_router, tags=[ApiSections.ANALYSES])
api.add_router("/samples", samples_router, tags=[ApiSections.SAMPLES])
api.add_router("/studies", studies_router, tags=[ApiSections.STUDIES])
api.add_router("/my-data", my_data_router, tags=[ApiSections.PRIVATE_DATA])
