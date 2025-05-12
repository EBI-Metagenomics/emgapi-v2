from typing import Optional, List
from genomes.schemas.GenomeBase import GenomeBase


class GenomeDetail(GenomeBase):
    geographic_origin: Optional[str]
    geographic_range: List[str]
    last_update: str
    first_created: str