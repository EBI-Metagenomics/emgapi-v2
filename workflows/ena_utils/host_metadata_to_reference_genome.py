from pydantic import BaseModel


class HostReferenceGenome(BaseModel):
    # TODO: consider promoting this to a pydantic setting and using a TOML file for content
    host_metadata: list[
        str
    ]  # e.g. "Gallus gallus" or "9031" – values in host-tax-id or host-scientific-name fields
    reference_genome: str  # e.g. "chicken_GCF_000002315.6"


TOMATO_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=[
        "tomato",
        "Solanum lycopersicum",
        "Solanum pimpinellifolium",
        "Solanum chmielewskii",
        "Solanum habrochaites",
        "Solanum pennellii",
        "Solanum peruvianum",
        "Lycopersicon esculentum",
        "4081",
        "4084",
        "62889",
        "62890",
        "28526",
        "4082",
    ],
    reference_genome="tomato",
)
CHICKEN_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["chicken", "Gallus gallus", "9031", "Galline"],
    reference_genome="chicken_GCF_000002315.6",
)
SALMON_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["salmon", "Salmo salar", "8030"],
    reference_genome="salmon_GCF_000233375.1",
)
COW_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["cow", "Bos taurus", "9913", "Bovine"],
    reference_genome="cow_GCA_000003205.6",
)
HONEYBEE_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["honeybee", "Apis mellifera", "7460", "Apine"],
    reference_genome="honeybee_GCF_003254395.2",
)
PIG_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["pig", "Sus scrofa", "9823", "Porcine"],
    reference_genome="pig_GCF_000003025.6",
)
MOUSE_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["mouse", "Mus musculus", "10090", "Murine"],
    reference_genome="mouse_GCA_000001635.9",
)
SHEEP_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["sheep", "Ovis aries", "9940", "Ovine"],
    reference_genome="sheep_GCA_000298735.2",
)
ZEBRAFISH_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["zebrafish", "Danio rerio", "7955", "Zebrafish"],
    reference_genome="zebrafish_GCF_000002035.6",
)
BARLEY_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=[
        "barley",
        "112509",
        "4513",
        "77009",
        "4516",
        "97361",
        "4519",
        "191506",
        "1288821",
        "Hordeum",
    ],
    reference_genome="barley_GCA_904849725.1",
)
MAIZE_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=[
        "maize",
        "Zea mays",
        "Zea perennis",
        "Zea diploperennis",
        "Zea luxurians",
        "Zea nicaraguensis",
        "381124",
        "4577",
        "4580",
        "4576",
        "15945",
        "1293079",
    ],
    reference_genome="maize_GCA_902167145.1",
)
RICE_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=[
        "rice",
        "Oryza sativa",
        "Oryza glaberrima",
        "Oryza rufipogon",
        "Oryza nivara",
        "Oryza barthii",
        "Oryza longistaminata",
        "Oryza meridionalis",
        "Oryza glumaepatula",
        "Oryza punctata",
        "Oryza officinalis",
        "Oryza malampuzhaensis",
        "Oryza schweinfurthiana",
        "Oryza minuta",
        "Oryza latifolia",
        "Oryza alta",
        "Oryza grandiglumis",
        "Oryza australiensis",
        "Oryza brachyantha",
        "Oryza granulata",
        "Oryza meyeriana",
        "Oryza ridleyi",
        "Oryza longiglumis",
        "4530",
        "4538",
        "4529",
        "4536",
        "65489",
        "4528",
        "40149",
        "40148",
        "4537",
        "4535",
        "127571",
        "63629",
        "4534",
        "52545",
        "29690",
        "4532",
        "4533",
        "110450",
        "83307",
        "83308",
        "83309",
    ],
    reference_genome="rice_GCF_001433935.1",
)
WHEAT_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=[
        "wheat",
        "Triticum",
        "4565",
        "376533",
        "860351",
        "52163",
        "295401",
        "69993",
        "85692",
        "4567",
        "376536",
        "376532",
        "376535",
        "69994",
        "58932",
        "4568",
        "376537",
        "501337",
        "77606",
        "58933",
        "4570",
        "4571",
        "4572",
        "77602",
    ],
    reference_genome="wheat_GCA_018294505.1",
)
COD_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["cod", "Gadus morhua", "8049"],
    reference_genome="cod_GCF_902167405.1",
)
RAT_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["rat", "Rattus norvegicus", "10116"],
    reference_genome="rat_GCA_015227675.2",
)
SOYBEAN_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["soybean", "Glycine max", "Glycine soja", "3847", "3848"],
    reference_genome="soybean_GCF_000004515.6",
)
RAINBOW_TROUT_REFERENCE_GENOME = HostReferenceGenome(
    host_metadata=["rainbow trout", "Oncorhynchus mykiss", "8022"],
    reference_genome="rainbow_trout_GCA_002163495.1",
)

KNOWN_REFERENCE_GENOMES = [
    TOMATO_REFERENCE_GENOME,
    CHICKEN_REFERENCE_GENOME,
    SALMON_REFERENCE_GENOME,
    COW_REFERENCE_GENOME,
    HONEYBEE_REFERENCE_GENOME,
    PIG_REFERENCE_GENOME,
    MOUSE_REFERENCE_GENOME,
    SHEEP_REFERENCE_GENOME,
    ZEBRAFISH_REFERENCE_GENOME,
    BARLEY_REFERENCE_GENOME,
    MAIZE_REFERENCE_GENOME,
    RICE_REFERENCE_GENOME,
    WHEAT_REFERENCE_GENOME,
    COD_REFERENCE_GENOME,
    RAT_REFERENCE_GENOME,
    SOYBEAN_REFERENCE_GENOME,
    RAINBOW_TROUT_REFERENCE_GENOME,
]


def get_reference_genome_for_host(host_taxon: str | None) -> HostReferenceGenome | None:
    for reference_genome in KNOWN_REFERENCE_GENOMES:
        for metadata_match in reference_genome.host_metadata:
            if metadata_match.lower() in host_taxon.lower():
                # match overspecified species names, like "Hordeum vulgare subsp. vulgare" etc
                return reference_genome
    return None
