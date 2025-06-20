from datetime import date
from typing import Optional

from pydantic import Field

from emgapiv2.enum_utils import FutureStrEnum
from workflows.ena_utils.abstract import _ENAQueryConditions


class ENAAnalysisQuery(_ENAQueryConditions):
    # From: https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result=analysis 2025/04/28
    # Some are controlled values not yet controlled here
    age: Optional[str] = Field(None, description="Age when the sample was taken")
    altitude: Optional[int] = Field(None, description="Altitude (m)")
    analysis_accession: Optional[str] = Field(None, description="accession number")
    analysis_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the analysis"
    )
    analysis_code_repository: Optional[str] = Field(
        None,
        description="Link to repository that contains the code used in the analysis.",
    )
    analysis_date: Optional[str] = Field(None, description="Date of analysis")
    analysis_description: Optional[str] = Field(
        None, description="Describes the analysis in detail"
    )
    analysis_protocol: Optional[str] = Field(
        None,
        description="Link to analysis protocol description, an overview of the full analysis including names, references and versions of any software employed. ",
    )
    analysis_title: Optional[str] = Field(
        None, description="brief sequence analysis description"
    )
    analysis_type: Optional[str] = Field(None, description="type of sequence analysis")
    assembly_software: Optional[str] = Field(None, description="Assembly software")
    assembly_type: Optional[str] = Field(None, description="analysis Assembly type")
    binning_software: Optional[str] = Field(None, description="Binning software")
    bio_material: Optional[str] = Field(
        None,
        description="identifier for biological material including institute and collection code",
    )
    broad_scale_environmental_context: Optional[str] = Field(
        None,
        description="Report the major environmental system the sample or specimen came from. The system(s) identified should have a coarse spatial grain, to provide the general environmental context of where the sampling was done (e.g. in the desert or a rainforest). We recommend using subclasses of EnvO’s biome class: http://purl.obolibrary.org/obo/ENVO_00000428. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.",
    )
    broker_name: Optional[str] = Field(None, description="broker name")
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
    collected_by: Optional[str] = Field(
        None, description="name of the person who collected the specimen"
    )
    completeness_score: Optional[int] = Field(
        None, description="Completeness score (%)"
    )
    contamination_score: Optional[int] = Field(
        None, description="Contamination score (%)"
    )
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
    experimental_factor: Optional[str] = Field(
        None, description="variable aspects of the experimental design"
    )
    first_created: Optional[date] = Field(None, description="date when first created")
    first_public: Optional[date] = Field(None, description="date when made public")
    germline: Optional[str] = Field(
        None,
        description="the sample is an unrearranged molecule that was inherited from the parental germline",
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
    pipeline_name: Optional[str] = Field(None, description="analysis pipeline name")
    pipeline_version: Optional[str] = Field(
        None, description="analysis pipeline version"
    )
    project_name: Optional[str] = Field(
        None,
        description="name of the project within which the sequencing was organized",
    )
    protocol_label: Optional[str] = Field(
        None, description="the protocol used to produce the sample"
    )
    pubmed_id: Optional[str] = Field(None, description="PubMed ID")
    reference_data_set_name: Optional[str] = Field(
        None, description="Taxonomic reference library analysis set name"
    )
    reference_data_set_version: Optional[str] = Field(
        None, description="Taxonomic reference library analysis set version"
    )
    reference_genome: Optional[str] = Field(
        None,
        description="The reference genome used in the analysis. Use &#x27;not applicable&#x27; if a reference genome was not required for this analysis type.",
    )
    related_analysis_accession: Optional[str] = Field(
        None, description="related analysis accession number"
    )
    run_accession: Optional[str] = Field(None, description="run accession number")
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
    sequencing_method: Optional[str] = Field(None, description="sequencing method used")
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
    variety: Optional[str] = Field(
        None,
        description="variety (varietas, a formal Linnaean rank) of organism from which sample was derived",
    )


class ENAAnalysisFields(FutureStrEnum):
    # from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=analysis 2025-04-28
    AGE = "age"  # Age when the sample was taken
    ALTITUDE = "altitude"  # Altitude (m)
    ANALYSIS_ACCESSION = "analysis_accession"  # accession number
    ANALYSIS_ALIAS = "analysis_alias"  # submitter's name for the analysis
    ANALYSIS_CODE_REPOSITORY = "analysis_code_repository"  # Link to repository that contains the code used in the analysis.
    ANALYSIS_DATE = "analysis_date"  # Date of analysis
    ANALYSIS_DESCRIPTION = "analysis_description"  # Describes the analysis in detail
    ANALYSIS_PROTOCOL = "analysis_protocol"  # Link to analysis protocol description, an overview of the full analysis including names, references and versions of any software employed.
    ANALYSIS_TITLE = "analysis_title"  # brief sequence analysis description
    ANALYSIS_TYPE = "analysis_type"  # type of sequence analysis
    ASSEMBLY_QUALITY = "assembly_quality"  # Quality of assembly
    ASSEMBLY_SOFTWARE = "assembly_software"  # Assembly software
    ASSEMBLY_TYPE = "assembly_type"  # analysis Assembly type
    BINNING_SOFTWARE = "binning_software"  # Binning software
    BIO_MATERIAL = "bio_material"  # identifier for biological material including institute and collection code
    BROAD_SCALE_ENVIRONMENTAL_CONTEXT = "broad_scale_environmental_context"  # Report the major environmental system the sample or specimen came from. The system(s) identified should have a coarse spatial grain, to provide the general environmental context of where the sampling was done (e.g. in the desert or a rainforest). We recommend using subclasses of EnvO’s biome class: http://purl.obolibrary.org/obo/ENVO_00000428. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.
    BROKER_NAME = "broker_name"  # broker name
    CELL_LINE = "cell_line"  # cell line from which the sample was obtained
    CELL_TYPE = "cell_type"  # cell type from which the sample was obtained
    CENTER_NAME = "center_name"  # Submitting center
    CHECKLIST = "checklist"  # ENA metadata reporting standard used to register the biosample (Checklist used)
    COLLECTED_BY = "collected_by"  # name of the person who collected the specimen
    COLLECTION_DATE = "collection_date"  # Time when specimen was collected
    COLLECTION_DATE_END = "collection_date_end"  # Time when specimen was collected
    COLLECTION_DATE_START = "collection_date_start"  # Time when specimen was collected
    COMPLETENESS_SCORE = "completeness_score"  # Completeness score (%)
    CONTAMINATION_SCORE = "contamination_score"  # Contamination score (%)
    COUNTRY = "country"  # locality of sample isolation: country names, oceans or seas, followed by regions and localities
    CULTIVAR = "cultivar"  # cultivar (cultivated variety) of plant from which sample was obtained
    CULTURE_COLLECTION = "culture_collection"  # identifier for the sample culture including institute and collection code
    DATAHUB = "datahub"  # DCC datahub name
    DEPTH = "depth"  # Depth (m)
    DESCRIPTION = "description"  # brief sequence description
    DEV_STAGE = "dev_stage"  # sample obtained from an organism in a specific developmental stage
    DISEASE = "disease"  # Disease associated with the sample
    ECOTYPE = "ecotype"  # a population within a given species displaying traits that reflect adaptation to a local habitat
    ELEVATION = "elevation"  # Elevation (m)
    ENVIRONMENT_BIOME = "environment_biome"  # Environment (Biome)
    ENVIRONMENT_FEATURE = "environment_feature"  # Environment (Feature)
    ENVIRONMENT_MATERIAL = "environment_material"  # Environment (Material)
    ENVIRONMENTAL_MEDIUM = "environmental_medium"  # Report the environmental material(s) immediately surrounding the sample or specimen at the time of sampling. We recommend using subclasses of 'environmental material' (http://purl.obolibrary.org/obo/ENVO_00010483). EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS . Terms from other OBO ontologies are permissible as long as they reference mass/volume nouns (e.g. air, water, blood) and not discrete, countable entities (e.g. a tree, a leaf, a table top).
    ENVIRONMENTAL_SAMPLE = "environmental_sample"  # identifies sequences derived by direct molecular isolation from an environmental DNA sample
    EXPERIMENT_ACCESSION = "experiment_accession"  # experiment accession number
    EXPERIMENTAL_FACTOR = (
        "experimental_factor"  # variable aspects of the experimental design
    )
    FIRST_CREATED = "first_created"  # date when first created
    FIRST_PUBLIC = "first_public"  # date when made public
    GENERATED_ASPERA = "generated_aspera"  # Aspera links for generated files. Use era-fasp or datahub name as username.
    GENERATED_BYTES = "generated_bytes"  # size (in bytes) of generated files
    GENERATED_FORMAT = "generated_format"  # Format for generated reads
    GENERATED_FTP = "generated_ftp"  # FTP links for generated files
    GENERATED_GALAXY = "generated_galaxy"  # Galaxy links for generated files
    GENERATED_MD5 = "generated_md5"  # MD5 checksum of generated files
    GERMLINE = "germline"  # the sample is an unrearranged molecule that was inherited from the parental germline
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
    INVESTIGATION_TYPE = (
        "investigation_type"  # the study type targeted by the sequencing
    )
    ISOLATE = "isolate"  # individual isolate from which sample was obtained
    ISOLATION_SOURCE = "isolation_source"  # describes the physical, environmental and/or local geographical source of the sample
    LAST_UPDATED = "last_updated"  # date when last updated
    LAT = "lat"  # Latitude
    LOCAL_ENVIRONMENTAL_CONTEXT = "local_environmental_context"  # Report the entity or entities which are in the sample or specimen’s local vicinity and which you believe have significant causal influences on your sample or specimen. We recommend using EnvO terms which are of smaller spatial grain than your entry for "broad-scale environmental context". Terms, such as anatomical sites, from other OBO Library ontologies which interoperate with EnvO (e.g. UBERON) are accepted in this field. EnvO documentation about how to use the field: https://github.com/EnvironmentOntology/envo/wiki/Using-ENVO-with-MIxS.
    LOCATION = "location"  # geographic location of isolation of the sample
    LOCATION_END = "location_end"  # latlon
    LOCATION_START = "location_start"  # latlon
    LON = "lon"  # Longitude
    MARINE_REGION = "marine_region"  # geographical origin of the sample as defined by the marine region
    MATING_TYPE = "mating_type"  # mating type of the organism from which the sequence was obtained
    NCBI_REPORTING_STANDARD = "ncbi_reporting_standard"  # NCBI metadata reporting standard used to register the biosample (Package used)
    PH = "ph"  # pH
    PIPELINE_NAME = "pipeline_name"  # analysis pipeline name
    PIPELINE_VERSION = "pipeline_version"  # analysis pipeline version
    PROJECT_NAME = (
        "project_name"  # name of the project within which the sequencing was organized
    )
    PROTOCOL_LABEL = "protocol_label"  # the protocol used to produce the sample
    PUBMED_ID = "pubmed_id"  # PubMed ID
    REFERENCE_DATA_SET_NAME = (
        "reference_data_set_name"  # Taxonomic reference library analysis set name
    )
    REFERENCE_DATA_SET_VERSION = (
        "reference_data_set_version"  # Taxonomic reference library analysis set version
    )
    REFERENCE_GENOME = "reference_genome"  # The reference genome used in the analysis. Use 'not applicable' if a reference genome was not required for this analysis type.
    RELATED_ANALYSIS_ACCESSION = (
        "related_analysis_accession"  # related analysis accession number
    )
    RUN_ACCESSION = "run_accession"  # run accession number
    SALINITY = "salinity"  # Salinity (PSU)
    SAMPLE_ACCESSION = "sample_accession"  # sample accession number
    SAMPLE_ALIAS = "sample_alias"  # submitter's name for the sample
    SAMPLE_CAPTURE_STATUS = "sample_capture_status"  # Sample capture status
    SAMPLE_COLLECTION = (
        "sample_collection"  # the method or device employed for collecting the sample
    )
    SAMPLE_DESCRIPTION = "sample_description"  # detailed sample description
    SAMPLE_MATERIAL = "sample_material"  # sample material label
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
    SEQUENCING_METHOD = "sequencing_method"  # sequencing method used
    SEROTYPE = "serotype"  # serological variety of a species characterized by its antigenic properties
    SEROVAR = "serovar"  # serological variety of a species (usually a prokaryote) characterized by its antigenic properties
    SEX = "sex"  # sex of the organism from which the sample was obtained
    SPECIMEN_VOUCHER = "specimen_voucher"  # identifier for the sample culture including institute and collection code
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
    TAG = "tag"  # Classification Tags
    TARGET_GENE = "target_gene"  # targeted gene or locus name for marker gene studies
    TAX_ID = "tax_id"  # NCBI taxonomic classification
    TAX_LINEAGE = "tax_lineage"  # Complete taxonomic lineage for an organism
    TAXONOMIC_CLASSIFICATION = "taxonomic_classification"  # Taxonomic classification
    TAXONOMIC_IDENTITY_MARKER = "taxonomic_identity_marker"  # Taxonomic identity marker
    TEMPERATURE = "temperature"  # Temperature (C)
    TISSUE_LIB = "tissue_lib"  # tissue library from which sample was obtained
    TISSUE_TYPE = "tissue_type"  # tissue type from which the sample was obtained
    VARIETY = "variety"  # variety (varietas, a formal Linnaean rank) of organism from which sample was derived
