from datetime import date
from typing import Optional

from pydantic import Field

from emgapiv2.enum_utils import FutureStrEnum
from workflows.ena_utils.abstract import _ENAQueryConditions


class ENAReadRunQuery(_ENAQueryConditions):
    # From: https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result=read_run 2025/04/28
    # Some are controlled values not yet controlled here
    age: Optional[str] = Field(None, description="Age when the sample was taken")
    altitude: Optional[int] = Field(None, description="Altitude (m)")
    assembly_software: Optional[str] = Field(None, description="Assembly software")
    base_count: Optional[int] = Field(None, description="number of base pairs")
    binning_software: Optional[str] = Field(None, description="Binning software")
    bio_material: Optional[str] = Field(
        None,
        description="identifier for biological material including institute and collection code",
    )
    bisulfite_protocol: Optional[str] = Field(None, description="text")
    broad_scale_environmental_context: Optional[str] = Field(
        None,
        description="Report the major environmental system the sample or specimen came from. The system(s) identified should have a coarse spatial grain, to provide the general environmental context of where the sampling was done (e.g. in the desert or a rainforest). We recommend using subclasses of EnvO’s biome class: http://purl.obolibrary.org/obo/ENVO_00000428. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.",
    )
    broker_name: Optional[str] = Field(None, description="broker name")
    cage_protocol: Optional[str] = Field(
        None, description="Link to the protocol for CAGE-seq experiments"
    )
    cell_line: Optional[str] = Field(
        None, description="cell line from which the sample was obtained"
    )
    cell_type: Optional[str] = Field(
        None, description="cell type from which the sample was obtained"
    )
    center_name: Optional[str] = Field(None, description="Submitting center")
    checklist: Optional[str] = Field(
        None,
        description="ENA metadata reporting standard used to register the biosample (Checklist used)",
    )
    chip_ab_provider: Optional[str] = Field(None, description="text")
    chip_protocol: Optional[str] = Field(None, description="text")
    chip_target: Optional[str] = Field(None, description="Chip target")
    collected_by: Optional[str] = Field(
        None, description="name of the person who collected the specimen"
    )
    completeness_score: Optional[int] = Field(
        None, description="Completeness score (%)"
    )
    contamination_score: Optional[int] = Field(
        None, description="Contamination score (%)"
    )
    control_experiment: Optional[str] = Field(None, description="Control experiment")
    country: Optional[str] = Field(
        None,
        description="locality of sample isolation: country names, oceans or seas, followed by regions and localities",
    )
    cultivar: Optional[str] = Field(
        None,
        description="cultivar (cultivated variety) of plant from which sample was obtained",
    )
    culture_collection: Optional[str] = Field(
        None,
        description="identifier for the sample culture including institute and collection code",
    )
    datahub: Optional[str] = Field(None, description="DCC datahub name")
    description: Optional[str] = Field(None, description="brief sequence description")
    dev_stage: Optional[str] = Field(
        None,
        description="sample obtained from an organism in a specific developmental stage",
    )
    disease: Optional[str] = Field(
        None, description="Disease associated with the sample"
    )
    dnase_protocol: Optional[str] = Field(None, description="text")
    ecotype: Optional[str] = Field(
        None,
        description="a population within a given species displaying traits that reflect adaptation to a local habitat",
    )
    elevation: Optional[int] = Field(None, description="Elevation (m)")
    environment_biome: Optional[str] = Field(None, description="Environment (Biome)")
    environment_feature: Optional[str] = Field(
        None, description="Environment (Feature)"
    )
    environment_material: Optional[str] = Field(
        None, description="Environment (Material)"
    )
    environmental_medium: Optional[str] = Field(
        None,
        description="Report the environmental material(s) immediately surrounding the sample or specimen at the time of sampling. We recommend using subclasses of &#x27;environmental material&#x27; (http://purl.obolibrary.org/obo/ENVO_00010483). EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS . Terms from other OBO ontologies are permissible as long as they reference mass/volume nouns (e.g. air, water, blood) and not discrete, countable entities (e.g. a tree, a leaf, a table top).",
    )
    environmental_sample: Optional[str] = Field(
        None,
        description="identifies sequences derived by direct molecular isolation from an environmental DNA sample",
    )
    experiment_accession: Optional[str] = Field(
        None, description="experiment accession number"
    )
    experiment_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the experiment"
    )
    experiment_target: Optional[str] = Field(None, description="text")
    experiment_title: Optional[str] = Field(None, description="brief experiment title")
    experimental_factor: Optional[str] = Field(
        None, description="variable aspects of the experimental design"
    )
    experimental_protocol: Optional[str] = Field(None, description="text")
    extraction_protocol: Optional[str] = Field(None, description="text")
    faang_library_selection: Optional[str] = Field(
        None, description="Library Selection for FAANG WGS/BS-Seq experiments"
    )
    first_created: Optional[date] = Field(None, description="date when first created")
    first_public: Optional[date] = Field(None, description="date when made public")
    germline: Optional[str] = Field(
        None,
        description="the sample is an unrearranged molecule that was inherited from the parental germline",
    )
    hi_c_protocol: Optional[str] = Field(
        None, description="Link to Hi-C Protocol for FAANG experiments"
    )
    host: Optional[str] = Field(
        None,
        description="natural (as opposed to laboratory) host to the organism from which sample was obtained",
    )
    host_body_site: Optional[str] = Field(
        None, description="name of body site from where the sample was obtained"
    )
    host_genotype: Optional[str] = Field(None, description="genotype of host")
    host_gravidity: Optional[str] = Field(
        None,
        description="whether or not subject is gravid, including date due or date post-conception where applicable",
    )
    host_growth_conditions: Optional[str] = Field(
        None, description="literature reference giving growth conditions of the host"
    )
    host_phenotype: Optional[str] = Field(None, description="phenotype of host")
    host_scientific_name: Optional[str] = Field(
        None,
        description="Scientific name of the natural (as opposed to laboratory) host to the organism from which sample was obtained",
    )
    host_sex: Optional[str] = Field(None, description="physical sex of the host")
    host_status: Optional[str] = Field(
        None, description="condition of host (eg. diseased or healthy)"
    )
    host_tax_id: Optional[int] = Field(None, description="NCBI taxon id of the host")
    identified_by: Optional[str] = Field(
        None, description="name of the taxonomist who identified the specimen"
    )
    instrument_model: Optional[str] = Field(
        None, description="instrument model used in sequencing experiment"
    )
    instrument_platform: Optional[str] = Field(
        None, description="instrument platform used in sequencing experiment"
    )
    investigation_type: Optional[str] = Field(
        None, description="the study type targeted by the sequencing"
    )
    isolate: Optional[str] = Field(
        None, description="individual isolate from which sample was obtained"
    )
    isolation_source: Optional[str] = Field(
        None,
        description="describes the physical, environmental and/or local geographical source of the sample",
    )
    last_updated: Optional[date] = Field(None, description="date when last updated")
    library_construction_protocol: Optional[str] = Field(
        None, description="Library construction protocol"
    )
    library_gen_protocol: Optional[str] = Field(None, description="text")
    library_layout: Optional[str] = Field(None, description="sequencing library layout")
    library_max_fragment_size: Optional[str] = Field(None, description="number")
    library_min_fragment_size: Optional[str] = Field(None, description="number")
    library_name: Optional[str] = Field(None, description="sequencing library name")
    library_pcr_isolation_protocol: Optional[str] = Field(None, description="text")
    library_prep_date: Optional[str] = Field(None, description="text")
    library_prep_date_format: Optional[str] = Field(None, description="text")
    library_prep_latitude: Optional[str] = Field(None, description="number")
    library_prep_location: Optional[str] = Field(None, description="text")
    library_prep_longitude: Optional[str] = Field(None, description="number")
    library_selection: Optional[str] = Field(
        None, description="method used to select or enrich the material being sequenced"
    )
    library_source: Optional[str] = Field(
        None, description="source material being sequenced"
    )
    library_strategy: Optional[str] = Field(
        None, description="sequencing technique intended for the library"
    )
    local_environmental_context: Optional[str] = Field(
        None,
        description="Report the entity or entities which are in the sample or specimen’s local vicinity and which you believe have significant causal influences on your sample or specimen. We recommend using EnvO terms which are of smaller spatial grain than your entry for &quot;broad-scale environmental context&quot;. Terms, such as anatomical sites, from other OBO Library ontologies which interoperate with EnvO (e.g. UBERON) are accepted in this field. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.",
    )
    location: Optional[str] = Field(
        None, description="geographic location of isolation of the sample"
    )
    location_end: Optional[str] = Field(None, description="latlon")
    location_start: Optional[str] = Field(None, description="latlon")
    marine_region: Optional[str] = Field(
        None,
        description="geographical origin of the sample as defined by the marine region",
    )
    mating_type: Optional[str] = Field(
        None,
        description="mating type of the organism from which the sequence was obtained",
    )
    ncbi_reporting_standard: Optional[str] = Field(
        None,
        description="NCBI metadata reporting standard used to register the biosample (Package used)",
    )
    nominal_length: Optional[int] = Field(
        None, description="average fragmentation size of paired reads"
    )
    nominal_sdev: Optional[int] = Field(
        None, description="standard deviation of fragmentation size of paired reads"
    )
    pcr_isolation_protocol: Optional[str] = Field(None, description="text")
    project_name: Optional[str] = Field(
        None,
        description="name of the project within which the sequencing was organized",
    )
    protocol_label: Optional[str] = Field(
        None, description="the protocol used to produce the sample"
    )
    read_count: Optional[int] = Field(None, description="number of reads")
    read_strand: Optional[str] = Field(None, description="text")
    restriction_enzyme: Optional[str] = Field(None, description="text")
    restriction_enzyme_target_sequence: Optional[str] = Field(
        None, description="The DNA sequence targeted by the restrict enzyme"
    )
    restriction_site: Optional[str] = Field(None, description="text")
    rna_integrity_num: Optional[str] = Field(None, description="number")
    rna_prep_3_protocol: Optional[str] = Field(None, description="text")
    rna_prep_5_protocol: Optional[str] = Field(None, description="text")
    rna_purity_230_ratio: Optional[str] = Field(None, description="number")
    rna_purity_280_ratio: Optional[str] = Field(None, description="number")
    rt_prep_protocol: Optional[str] = Field(None, description="text")
    run_accession: Optional[str] = Field(None, description="accession number")
    run_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the run"
    )
    salinity: Optional[int] = Field(None, description="Salinity (PSU)")
    sample_accession: Optional[str] = Field(None, description="sample accession number")
    sample_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the sample"
    )
    sample_capture_status: Optional[str] = Field(
        None, description="Sample capture status"
    )
    sample_collection: Optional[str] = Field(
        None, description="the method or device employed for collecting the sample"
    )
    sample_description: Optional[str] = Field(
        None, description="detailed sample description"
    )
    sample_material: Optional[str] = Field(None, description="sample material label")
    sample_prep_interval: Optional[str] = Field(None, description="number")
    sample_prep_interval_units: Optional[str] = Field(None, description="text")
    sample_storage: Optional[str] = Field(None, description="text")
    sample_storage_processing: Optional[str] = Field(None, description="text")
    sample_title: Optional[str] = Field(None, description="brief sample title")
    sampling_campaign: Optional[str] = Field(
        None, description="the activity within which this sample was collected"
    )
    sampling_platform: Optional[str] = Field(
        None,
        description="the large infrastructure from which this sample was collected",
    )
    sampling_site: Optional[str] = Field(
        None, description="the site/station where this sample was collection"
    )
    scientific_name: Optional[str] = Field(
        None, description="scientific name of an organism"
    )
    secondary_project: Optional[str] = Field(None, description="Secondary project")
    secondary_sample_accession: Optional[str] = Field(
        None, description="secondary sample accession number"
    )
    secondary_study_accession: Optional[str] = Field(
        None, description="secondary study accession number"
    )
    sequencing_date: Optional[str] = Field(None, description="text")
    sequencing_date_format: Optional[str] = Field(None, description="text")
    sequencing_location: Optional[str] = Field(None, description="text")
    sequencing_longitude: Optional[str] = Field(None, description="number")
    sequencing_method: Optional[str] = Field(None, description="sequencing method used")
    sequencing_primer_catalog: Optional[str] = Field(
        None,
        description="The catalog from which the sequencing primer library was purchased",
    )
    sequencing_primer_lot: Optional[str] = Field(
        None, description="The lot identifier of the sequencing primer library"
    )
    sequencing_primer_provider: Optional[str] = Field(
        None,
        description="The name of the company, laboratory or person that provided the sequencing primer library",
    )
    serotype: Optional[str] = Field(
        None,
        description="serological variety of a species characterized by its antigenic properties",
    )
    serovar: Optional[str] = Field(
        None,
        description="serological variety of a species (usually a prokaryote) characterized by its antigenic properties",
    )
    sex: Optional[str] = Field(
        None, description="sex of the organism from which the sample was obtained"
    )
    specimen_voucher: Optional[str] = Field(
        None,
        description="identifier for the sample culture including institute and collection code",
    )
    status: Optional[int] = Field(None, description="Status")
    strain: Optional[str] = Field(
        None, description="strain from which sample was obtained"
    )
    study_accession: Optional[str] = Field(None, description="study accession number")
    study_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the study"
    )
    study_title: Optional[str] = Field(
        None, description="brief sequencing study description"
    )
    sub_species: Optional[str] = Field(
        None,
        description="name of sub-species of organism from which sample was obtained",
    )
    sub_strain: Optional[str] = Field(
        None,
        description="name or identifier of a genetically or otherwise modified strain from which sample was obtained",
    )
    submission_accession: Optional[str] = Field(
        None, description="submission accession number"
    )
    submission_tool: Optional[str] = Field(None, description="Submission tool")
    submitted_format: Optional[str] = Field(
        None, description="format of submitted reads"
    )
    submitted_host_sex: Optional[str] = Field(
        None, description="physical sex of the host"
    )
    submitted_md5: Optional[str] = Field(
        None, description="MD5 checksum of submitted files"
    )
    submitted_read_type: Optional[str] = Field(
        None, description="submitted FASTQ read type"
    )
    tag: Optional[str] = Field(None, description="Classification Tags")
    target_gene: Optional[str] = Field(
        None, description="targeted gene or locus name for marker gene studies"
    )
    tax_id: Optional[str] = Field(None, description="NCBI taxonomic classification")
    taxonomic_classification: Optional[str] = Field(
        None, description="Taxonomic classification"
    )
    taxonomic_identity_marker: Optional[str] = Field(
        None, description="Taxonomic identity marker"
    )
    temperature: Optional[int] = Field(None, description="Temperature (C)")
    tissue_lib: Optional[str] = Field(
        None, description="tissue library from which sample was obtained"
    )
    tissue_type: Optional[str] = Field(
        None, description="tissue type from which the sample was obtained"
    )
    transposase_protocol: Optional[str] = Field(None, description="text")
    variety: Optional[str] = Field(
        None,
        description="variety (varietas, a formal Linnaean rank) of organism from which sample was derived",
    )


class ENAReadRunFields(FutureStrEnum):
    # from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=read_run 2025-04-28
    AGE = "age"  # Age when the sample was taken
    ALIGNED = "aligned"  # boolean
    ALTITUDE = "altitude"  # Altitude (m)
    ASSEMBLY_QUALITY = "assembly_quality"  # Quality of assembly
    ASSEMBLY_SOFTWARE = "assembly_software"  # Assembly software
    BAM_ASPERA = "bam_aspera"  # Aspera links for generated bam files. Use era-fasp or datahub name as username.
    BAM_BYTES = "bam_bytes"  # size (in bytes) of generated BAM files
    BAM_FTP = "bam_ftp"  # FTP links for generated bam files
    BAM_GALAXY = "bam_galaxy"  # Galaxy links for generated bam files
    BAM_MD5 = "bam_md5"  # MD5 checksum of generated BAM files
    BASE_COUNT = "base_count"  # number of base pairs
    BINNING_SOFTWARE = "binning_software"  # Binning software
    BIO_MATERIAL = "bio_material"  # identifier for biological material including institute and collection code
    BISULFITE_PROTOCOL = "bisulfite_protocol"  # text
    BROAD_SCALE_ENVIRONMENTAL_CONTEXT = "broad_scale_environmental_context"  # Report the major environmental system the sample or specimen came from. The system(s) identified should have a coarse spatial grain, to provide the general environmental context of where the sampling was done (e.g. in the desert or a rainforest). We recommend using subclasses of EnvO’s biome class: http://purl.obolibrary.org/obo/ENVO_00000428. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.
    BROKER_NAME = "broker_name"  # broker name
    CAGE_PROTOCOL = "cage_protocol"  # Link to the protocol for CAGE-seq experiments
    CELL_LINE = "cell_line"  # cell line from which the sample was obtained
    CELL_TYPE = "cell_type"  # cell type from which the sample was obtained
    CENTER_NAME = "center_name"  # Submitting center
    CHECKLIST = "checklist"  # ENA metadata reporting standard used to register the biosample (Checklist used)
    CHIP_AB_PROVIDER = "chip_ab_provider"  # text
    CHIP_PROTOCOL = "chip_protocol"  # text
    CHIP_TARGET = "chip_target"  # Chip target
    COLLECTED_BY = "collected_by"  # name of the person who collected the specimen
    COLLECTION_DATE = "collection_date"  # Time when specimen was collected
    COLLECTION_DATE_END = "collection_date_end"  # Time when specimen was collected
    COLLECTION_DATE_START = "collection_date_start"  # Time when specimen was collected
    COMPLETENESS_SCORE = "completeness_score"  # Completeness score (%)
    CONTAMINATION_SCORE = "contamination_score"  # Contamination score (%)
    CONTROL_EXPERIMENT = "control_experiment"  # Control experiment
    COUNTRY = "country"  # locality of sample isolation: country names, oceans or seas, followed by regions and localities
    CULTIVAR = "cultivar"  # cultivar (cultivated variety) of plant from which sample was obtained
    CULTURE_COLLECTION = "culture_collection"  # identifier for the sample culture including institute and collection code
    DATAHUB = "datahub"  # DCC datahub name
    DEPTH = "depth"  # Depth (m)
    DESCRIPTION = "description"  # brief sequence description
    DEV_STAGE = "dev_stage"  # sample obtained from an organism in a specific developmental stage
    DISEASE = "disease"  # Disease associated with the sample
    DNASE_PROTOCOL = "dnase_protocol"  # text
    ECOTYPE = "ecotype"  # a population within a given species displaying traits that reflect adaptation to a local habitat
    ELEVATION = "elevation"  # Elevation (m)
    ENVIRONMENT_BIOME = "environment_biome"  # Environment (Biome)
    ENVIRONMENT_FEATURE = "environment_feature"  # Environment (Feature)
    ENVIRONMENT_MATERIAL = "environment_material"  # Environment (Material)
    ENVIRONMENTAL_MEDIUM = "environmental_medium"  # Report the environmental material(s) immediately surrounding the sample or specimen at the time of sampling. We recommend using subclasses of 'environmental material' (http://purl.obolibrary.org/obo/ENVO_00010483). EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS . Terms from other OBO ontologies are permissible as long as they reference mass/volume nouns (e.g. air, water, blood) and not discrete, countable entities (e.g. a tree, a leaf, a table top).
    ENVIRONMENTAL_SAMPLE = "environmental_sample"  # identifies sequences derived by direct molecular isolation from an environmental DNA sample
    EXPERIMENT_ACCESSION = "experiment_accession"  # experiment accession number
    EXPERIMENT_ALIAS = "experiment_alias"  # submitter's name for the experiment
    EXPERIMENT_TARGET = "experiment_target"  # text
    EXPERIMENT_TITLE = "experiment_title"  # brief experiment title
    EXPERIMENTAL_FACTOR = (
        "experimental_factor"  # variable aspects of the experimental design
    )
    EXPERIMENTAL_PROTOCOL = "experimental_protocol"  # text
    EXTRACTION_PROTOCOL = "extraction_protocol"  # text
    FAANG_LIBRARY_SELECTION = (
        "faang_library_selection"  # Library Selection for FAANG WGS/BS-Seq experiments
    )
    FASTQ_ASPERA = "fastq_aspera"  # Aspera links for fastq files. Use era-fasp or datahub name as username.
    FASTQ_BYTES = "fastq_bytes"  # size (in bytes) of FASTQ files
    FASTQ_FTP = "fastq_ftp"  # FTP links for fastq files
    FASTQ_GALAXY = "fastq_galaxy"  # Galaxy links for fastq files
    FASTQ_MD5 = "fastq_md5"  # MD5 checksum of FASTQ files
    FILE_LOCATION = "file_location"  # text
    FIRST_CREATED = "first_created"  # date when first created
    FIRST_PUBLIC = "first_public"  # date when made public
    GERMLINE = "germline"  # the sample is an unrearranged molecule that was inherited from the parental germline
    HI_C_PROTOCOL = "hi_c_protocol"  # Link to Hi-C Protocol for FAANG experiments
    HOST = "host"  # natural (as opposed to laboratory) host to the organism from which sample was obtained
    HOST_BODY_SITE = (
        "host_body_site"  # name of body site from where the sample was obtained
    )
    HOST_GENOTYPE = "host_genotype"  # genotype of host
    HOST_GRAVIDITY = "host_gravidity"  # whether or not subject is gravid, including date due or date post-conception where applicable
    HOST_GROWTH_CONDITIONS = "host_growth_conditions"  # literature reference giving growth conditions of the host
    HOST_PHENOTYPE = "host_phenotype"  # phenotype of host
    HOST_SCIENTIFIC_NAME = "host_scientific_name"  # Scientific name of the natural (as opposed to laboratory) host to the organism from which sample was obtained
    HOST_SEX = "host_sex"  # physical sex of the host
    HOST_STATUS = "host_status"  # condition of host (eg. diseased or healthy)
    HOST_TAX_ID = "host_tax_id"  # NCBI taxon id of the host
    IDENTIFIED_BY = (
        "identified_by"  # name of the taxonomist who identified the specimen
    )
    INSTRUMENT_MODEL = (
        "instrument_model"  # instrument model used in sequencing experiment
    )
    INSTRUMENT_PLATFORM = (
        "instrument_platform"  # instrument platform used in sequencing experiment
    )
    INVESTIGATION_TYPE = (
        "investigation_type"  # the study type targeted by the sequencing
    )
    ISOLATE = "isolate"  # individual isolate from which sample was obtained
    ISOLATION_SOURCE = "isolation_source"  # describes the physical, environmental and/or local geographical source of the sample
    LAST_UPDATED = "last_updated"  # date when last updated
    LAT = "lat"  # Latitude
    LIBRARY_CONSTRUCTION_PROTOCOL = (
        "library_construction_protocol"  # Library construction protocol
    )
    LIBRARY_GEN_PROTOCOL = "library_gen_protocol"  # text
    LIBRARY_LAYOUT = "library_layout"  # sequencing library layout
    LIBRARY_MAX_FRAGMENT_SIZE = "library_max_fragment_size"  # number
    LIBRARY_MIN_FRAGMENT_SIZE = "library_min_fragment_size"  # number
    LIBRARY_NAME = "library_name"  # sequencing library name
    LIBRARY_PCR_ISOLATION_PROTOCOL = "library_pcr_isolation_protocol"  # text
    LIBRARY_PREP_DATE = "library_prep_date"  # text
    LIBRARY_PREP_DATE_FORMAT = "library_prep_date_format"  # text
    LIBRARY_PREP_LATITUDE = "library_prep_latitude"  # number
    LIBRARY_PREP_LOCATION = "library_prep_location"  # text
    LIBRARY_PREP_LONGITUDE = "library_prep_longitude"  # number
    LIBRARY_SELECTION = "library_selection"  # method used to select or enrich the material being sequenced
    LIBRARY_SOURCE = "library_source"  # source material being sequenced
    LIBRARY_STRATEGY = (
        "library_strategy"  # sequencing technique intended for the library
    )
    LOCAL_ENVIRONMENTAL_CONTEXT = "local_environmental_context"  # Report the entity or entities which are in the sample or specimen’s local vicinity and which you believe have significant causal influences on your sample or specimen. We recommend using EnvO terms which are of smaller spatial grain than your entry for "broad-scale environmental context". Terms, such as anatomical sites, from other OBO Library ontologies which interoperate with EnvO (e.g. UBERON) are accepted in this field. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.
    LOCATION = "location"  # geographic location of isolation of the sample
    LOCATION_END = "location_end"  # latlon
    LOCATION_START = "location_start"  # latlon
    LON = "lon"  # Longitude
    MARINE_REGION = "marine_region"  # geographical origin of the sample as defined by the marine region
    MATING_TYPE = "mating_type"  # mating type of the organism from which the sequence was obtained
    NCBI_REPORTING_STANDARD = "ncbi_reporting_standard"  # NCBI metadata reporting standard used to register the biosample (Package used)
    NOMINAL_LENGTH = "nominal_length"  # average fragmentation size of paired reads
    NOMINAL_SDEV = (
        "nominal_sdev"  # standard deviation of fragmentation size of paired reads
    )
    PCR_ISOLATION_PROTOCOL = "pcr_isolation_protocol"  # text
    PH = "ph"  # pH
    PROJECT_NAME = (
        "project_name"  # name of the project within which the sequencing was organized
    )
    PROTOCOL_LABEL = "protocol_label"  # the protocol used to produce the sample
    READ_COUNT = "read_count"  # number of reads
    READ_STRAND = "read_strand"  # text
    RESTRICTION_ENZYME = "restriction_enzyme"  # text
    RESTRICTION_ENZYME_TARGET_SEQUENCE = "restriction_enzyme_target_sequence"  # The DNA sequence targeted by the restrict enzyme
    RESTRICTION_SITE = "restriction_site"  # text
    RNA_INTEGRITY_NUM = "rna_integrity_num"  # number
    RNA_PREP_3_PROTOCOL = "rna_prep_3_protocol"  # text
    RNA_PREP_5_PROTOCOL = "rna_prep_5_protocol"  # text
    RNA_PURITY_230_RATIO = "rna_purity_230_ratio"  # number
    RNA_PURITY_280_RATIO = "rna_purity_280_ratio"  # number
    RT_PREP_PROTOCOL = "rt_prep_protocol"  # text
    RUN_ACCESSION = "run_accession"  # accession number
    RUN_ALIAS = "run_alias"  # submitter's name for the run
    RUN_DATE = "run_date"  # date
    SALINITY = "salinity"  # Salinity (PSU)
    SAMPLE_ACCESSION = "sample_accession"  # sample accession number
    SAMPLE_ALIAS = "sample_alias"  # submitter's name for the sample
    SAMPLE_CAPTURE_STATUS = "sample_capture_status"  # Sample capture status
    SAMPLE_COLLECTION = (
        "sample_collection"  # the method or device employed for collecting the sample
    )
    SAMPLE_DESCRIPTION = "sample_description"  # detailed sample description
    SAMPLE_MATERIAL = "sample_material"  # sample material label
    SAMPLE_PREP_INTERVAL = "sample_prep_interval"  # number
    SAMPLE_PREP_INTERVAL_UNITS = "sample_prep_interval_units"  # text
    SAMPLE_STORAGE = "sample_storage"  # text
    SAMPLE_STORAGE_PROCESSING = "sample_storage_processing"  # text
    SAMPLE_TITLE = "sample_title"  # brief sample title
    SAMPLING_CAMPAIGN = (
        "sampling_campaign"  # the activity within which this sample was collected
    )
    SAMPLING_PLATFORM = "sampling_platform"  # the large infrastructure from which this sample was collected
    SAMPLING_SITE = "sampling_site"  # the site/station where this sample was collection
    SCIENTIFIC_NAME = "scientific_name"  # scientific name of an organism
    SECONDARY_PROJECT = "secondary_project"  # Secondary project
    SECONDARY_SAMPLE_ACCESSION = (
        "secondary_sample_accession"  # secondary sample accession number
    )
    SECONDARY_STUDY_ACCESSION = (
        "secondary_study_accession"  # secondary study accession number
    )
    SEQUENCING_DATE = "sequencing_date"  # text
    SEQUENCING_DATE_FORMAT = "sequencing_date_format"  # text
    SEQUENCING_LOCATION = "sequencing_location"  # text
    SEQUENCING_LONGITUDE = "sequencing_longitude"  # number
    SEQUENCING_METHOD = "sequencing_method"  # sequencing method used
    SEQUENCING_PRIMER_CATALOG = "sequencing_primer_catalog"  # The catalog from which the sequencing primer library was purchased
    SEQUENCING_PRIMER_LOT = (
        "sequencing_primer_lot"  # The lot identifier of the sequencing primer library
    )
    SEQUENCING_PRIMER_PROVIDER = "sequencing_primer_provider"  # The name of the company, laboratory or person that provided the sequencing primer library
    SEROTYPE = "serotype"  # serological variety of a species characterized by its antigenic properties
    SEROVAR = "serovar"  # serological variety of a species (usually a prokaryote) characterized by its antigenic properties
    SEX = "sex"  # sex of the organism from which the sample was obtained
    SPECIMEN_VOUCHER = "specimen_voucher"  # identifier for the sample culture including institute and collection code
    SRA_ASPERA = "sra_aspera"  # Aspera links for SRA data files. Use era-fasp or datahub name as username.
    SRA_BYTES = "sra_bytes"  # size (in bytes) of SRA files
    SRA_FTP = "sra_ftp"  # FTP links for SRA data files
    SRA_GALAXY = "sra_galaxy"  # Galaxy links for SRA data files
    SRA_MD5 = "sra_md5"  # MD5 checksum of atchived files
    STATUS = "status"  # Status
    STRAIN = "strain"  # strain from which sample was obtained
    STUDY_ACCESSION = "study_accession"  # study accession number
    STUDY_ALIAS = "study_alias"  # submitter's name for the study
    STUDY_TITLE = "study_title"  # brief sequencing study description
    SUB_SPECIES = (
        "sub_species"  # name of sub-species of organism from which sample was obtained
    )
    SUB_STRAIN = "sub_strain"  # name or identifier of a genetically or otherwise modified strain from which sample was obtained
    SUBMISSION_ACCESSION = "submission_accession"  # submission accession number
    SUBMISSION_TOOL = "submission_tool"  # Submission tool
    SUBMITTED_ASPERA = "submitted_aspera"  # Aspera links for submitted files. Use era-fasp or datahub name as username.
    SUBMITTED_BYTES = "submitted_bytes"  # size (in bytes) of submitted files
    SUBMITTED_FORMAT = "submitted_format"  # format of submitted reads
    SUBMITTED_FTP = "submitted_ftp"  # FTP links for submitted files
    SUBMITTED_GALAXY = "submitted_galaxy"  # Galaxy links for submitted files
    SUBMITTED_HOST_SEX = "submitted_host_sex"  # physical sex of the host
    SUBMITTED_MD5 = "submitted_md5"  # MD5 checksum of submitted files
    SUBMITTED_READ_TYPE = "submitted_read_type"  # submitted FASTQ read type
    TAG = "tag"  # Classification Tags
    TARGET_GENE = "target_gene"  # targeted gene or locus name for marker gene studies
    TAX_ID = "tax_id"  # NCBI taxonomic classification
    TAX_LINEAGE = "tax_lineage"  # Complete taxonomic lineage for an organism
    TAXONOMIC_CLASSIFICATION = "taxonomic_classification"  # Taxonomic classification
    TAXONOMIC_IDENTITY_MARKER = "taxonomic_identity_marker"  # Taxonomic identity marker
    TEMPERATURE = "temperature"  # Temperature (C)
    TISSUE_LIB = "tissue_lib"  # tissue library from which sample was obtained
    TISSUE_TYPE = "tissue_type"  # tissue type from which the sample was obtained
    TRANSPOSASE_PROTOCOL = "transposase_protocol"  # text
    VARIETY = "variety"  # variety (varietas, a formal Linnaean rank) of organism from which sample was derived
