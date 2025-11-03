from textwrap import dedent

from ninja_extra import NinjaExtraAPI

from emgapiv2.api.schema_utils import OpenApiKeywords, ApiSections
from .analyses import AnalysisController
from .biome import BiomeController
from .private import MyDataController
from .publications import PublicationController
from .samples import SampleController
from .studies import StudyController
from .super_studies import SuperStudyController
from .genomes import GenomeController
from .token_controller import WebinJwtController

# Avoid circular imports between analyses and genomes apps' schemas by resolving forward references here
from analyses.schemas import Biome, MGnifyAnalysisDownloadFile  # noqa
from genomes.schemas import (
    GenomeDetail,
    GenomeList,
    GenomeCatalogueDetail,
    GenomeCatalogueList,
)

GenomeList.model_rebuild()
GenomeDetail.model_rebuild()
GenomeCatalogueList.model_rebuild()
GenomeCatalogueDetail.model_rebuild()

api = NinjaExtraAPI(
    title="MGnify API",
    description=dedent(
        """\
        **The API for [MGnify](https://www.ebi.ac.uk/metagenomics), EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.**

        ## Endpoints
        API endpoints (URLs) are available for all data types stored in MGnify.
        In general, there are list endpoints (ending `/`) and detail endpoints (ending `/<accession>`) for individually accessioned data.
        Where a data type is not accessioned, alternative natural identifiers are used (like a short `slug` for Super Studies).
        Commonly used relationships are available as nested list endpoints, like `studies/{accession}/samples` for the samples belonging to a study.

        ## Data format
        The API returns JSON responses.
        List endpoints return data in the format {count: 10, items: [{...}, ...]} (unless specified otherwise)
        Detail endpoints return a single object `{...}`.

        ## Data availability
        Most data are public, available without authentication.
        Private data is available only to authenticated users, and the authentication is based on Webin credentials from ENA.
        To use these, a JWT token must be obtained for the Webin credentials, using the `/auth` endpoints.
    """
    ),
    urls_namespace="api",
    version="2.0.0",
    docs_url="/",
    openapi_extra={
        OpenApiKeywords.EXTERNAL_DOCS: {
            OpenApiKeywords.DESCRIPTION: "MGnify documentation",
            OpenApiKeywords.URL: "https://docs.mgnify.org/",
        },
        "info": {
            "contact": {
                OpenApiKeywords.NAME: "MGnify Support",
                OpenApiKeywords.URL: "https://www.ebi.ac.uk/about/contact/support/metagenomics",
            },
            "termsOfService": "https://www.ebi.ac.uk/about/terms-of-use",
        },
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
                OpenApiKeywords.NAME: ApiSections.PUBLICATIONS,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    Publications (e.g. journal articles) may describe or analyse the content of MGnify Studies
                    or their corresponding datasets in ENA.
                    """
                ),
            },
            {
                OpenApiKeywords.NAME: ApiSections.GENOMES,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    MGnify Genomes are annotated draft genomes based on either isolates, or metagenome-assembled genomes (MAGs).
                    They are arranged in biome-specific catalogues.
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
                OpenApiKeywords.NAME: ApiSections.MISC,
                OpenApiKeywords.DESCRIPTION: dedent(
                    """
                    Other miscellaneous endpoints in support of the API.
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
        ],
    },
)

api.register_controllers(AnalysisController)
api.register_controllers(PublicationController)
api.register_controllers(SampleController)
api.register_controllers(StudyController)
api.register_controllers(SuperStudyController)
api.register_controllers(GenomeController)
api.register_controllers(BiomeController)
api.register_controllers(MyDataController)

# Private data auth token provider (Webin JWTs)
api.register_controllers(WebinJwtController)
