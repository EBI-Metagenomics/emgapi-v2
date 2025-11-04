from typing import Optional, ClassVar

from ninja import Query
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

import analyses.models
from analyses.schemas import Biome
from emgapiv2.api import ApiSections
from emgapiv2.api.schema_utils import BiomeFilter


class BiomeListFilters(BiomeFilter):
    LOOKUP_STRING: ClassVar[str] = "path__descendants"  # for BiomeFilter

    max_depth: Optional[int] = Field(
        None,
        ge=1,
        description="Maximum depth of the biome lineage to include, e.g. `root` is 1 and `root:Host-Associated:Human` is level 3",
        q="path__depth__lte",
    )


@api_controller("biomes", tags=[ApiSections.MISC])
class BiomeController:
    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[Biome],
        summary="List all biomes",
        description="List all biomes in the MGnify database.",
        operation_id="list_mgnify_biomes",
    )
    @paginate()
    def list_mgnify_biomes(
        self,
        filters: BiomeListFilters = Query(...),
    ):
        qs = analyses.models.Biome.objects.all()
        qs = filters.filter(qs)
        return qs
