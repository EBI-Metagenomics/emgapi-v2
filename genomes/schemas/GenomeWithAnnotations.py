from typing import Dict, Any
from ninja import Schema


class GenomeWithAnnotations(Schema):
    accession: str
    annotations: Dict[str, Any]
