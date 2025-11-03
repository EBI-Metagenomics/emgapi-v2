from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import Biome
from emgapiv2.api import ApiSections


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
    ):
        qs = analyses.models.Biome.objects.all()
        return qs
