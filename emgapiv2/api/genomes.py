from typing import List, Dict, Any
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated
from ninja import Router

from genomes.models import Genome
from genomes.schemas import GenomeDetail, GenomeList, GenomeWithAnnotations

router = RouterPaginated(tags=["Genomes"])


@router.get("/", response=List[GenomeList], summary="List all genomes", operation_id="list_genomes")
def list_genomes(request):
    return Genome.objects.select_related("biome", "catalogue").prefetch_related("pangenome_geographic_range")


@router.get("/{accession}", response=GenomeDetail, summary="Get genome by accession", operation_id="get_genome")
def get_genome(request, accession: str):
    genome = get_object_or_404(
        Genome.objects.select_related("biome", "geo_origin", "catalogue").prefetch_related("pangenome_geographic_range"),
        accession=accession
    )
    return {
        "accession": genome.accession,
        "ena_genome_accession": genome.ena_genome_accession,
        "ena_sample_accession": genome.ena_sample_accession,
        "ncbi_genome_accession": genome.ncbi_genome_accession,
        "img_genome_accession": genome.img_genome_accession,
        "patric_genome_accession": genome.patric_genome_accession,
        "biome_id": genome.biome.id,
        "length": genome.length,
        "num_contigs": genome.num_contigs,
        "n_50": genome.n_50,
        "gc_content": genome.gc_content,
        "type": genome.type,
        "completeness": genome.completeness,
        "contamination": genome.contamination,
        "catalogue_id": genome.catalogue.id,
        "geographic_origin": genome.geographic_origin,
        "geographic_range": genome.geographic_range,
        "last_update": genome.last_update.isoformat(),
        "first_created": genome.first_created.isoformat(),
    }


@router.get("/{accession}/annotations", response=GenomeWithAnnotations, summary="Get genome annotations by accession", operation_id="get_genome_annotations")
def get_genome_annotations(request, accession: str):
    genome = get_object_or_404(
        Genome.objects_and_annotations,
        accession=accession
    )
    return {
        "accession": genome.accession,
        "annotations": genome.annotations,
    }
