from contextlib import contextmanager
from typing import List, Optional, TYPE_CHECKING

import pymongo
from django.conf import settings
from prefect import get_run_logger, task
from pymongo.synchronous.collection import Collection
from sqlalchemy import Boolean, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)

from analyses.base_models.with_downloads_models import DownloadFileType, DownloadType

if TYPE_CHECKING:
    import analyses.models


class LegacyEMGBase(DeclarativeBase):
    """
    Legacy models in EMG Mysql DB are defined with SQLAlechmy,
    so that we can use modern Django without worrying about
    no-longer-supported MySQL engines.

    These are just convenience ORM models, to help
    with importing legacy data into the v2 API's db.
    """

    pass


class LegacyBiome(LegacyEMGBase):
    __tablename__ = "BIOME_HIERARCHY_TREE"

    id: Mapped[int] = mapped_column("BIOME_ID", Integer, primary_key=True)
    lineage: Mapped[str] = mapped_column("LINEAGE", String)
    biome_name: Mapped[str] = mapped_column("BIOME_NAME", String)

    studies: Mapped["LegacyStudy"] = relationship("LegacyStudy", back_populates="biome")


class LegacyStudy(LegacyEMGBase):
    __tablename__ = "STUDY"

    id: Mapped[int] = mapped_column("STUDY_ID", Integer, primary_key=True)
    centre_name: Mapped[str] = mapped_column("CENTRE_NAME", String)
    study_name: Mapped[str] = mapped_column("STUDY_NAME", String)
    ext_study_id: Mapped[str] = mapped_column("EXT_STUDY_ID", String)
    project_id: Mapped[str] = mapped_column("PROJECT_ID", String)

    is_suppressed: Mapped[bool] = mapped_column("IS_SUPPRESSED", Boolean)
    is_private: Mapped[bool] = mapped_column("IS_PRIVATE", Boolean)

    analysis_jobs: Mapped[list["LegacyAnalysisJob"]] = relationship(
        back_populates="study"
    )

    biome_id: Mapped[int] = mapped_column(
        "BIOME_ID", ForeignKey("BIOME_HIERARCHY_TREE.BIOME_ID")
    )
    biome: Mapped["LegacyBiome"] = relationship("LegacyBiome", back_populates="studies")


class LegacySample(LegacyEMGBase):
    __tablename__ = "SAMPLE"

    sample_id: Mapped[int] = mapped_column("SAMPLE_ID", Integer, primary_key=True)
    ext_sample_id: Mapped[str] = mapped_column("EXT_SAMPLE_ID", String)
    primary_accession: Mapped[str] = mapped_column("PRIMARY_ACCESSION", String)

    analysis_jobs: Mapped[list["LegacyAnalysisJob"]] = relationship(
        back_populates="sample"
    )


class LegacyRun(LegacyEMGBase):
    __tablename__ = "RUN"
    run_id: Mapped[int] = mapped_column("RUN_ID", Integer, primary_key=True)
    accession: Mapped[str] = mapped_column("ACCESSION", String)
    secondary_accession: Mapped[str] = mapped_column("SECONDARY_ACCESSION", String)
    instrument_platform: Mapped[str] = mapped_column("INSTRUMENT_Platform", String)
    instrument_model: Mapped[str] = mapped_column("INSTRUMENT_MODEL", String)

    sample_id: Mapped[int] = mapped_column("SAMPLE_ID", ForeignKey("SAMPLE.SAMPLE_ID"))
    sample: Mapped["LegacySample"] = relationship("LegacySample")

    study_id: Mapped[int] = mapped_column("STUDY_ID", ForeignKey("STUDY.STUDY_ID"))
    study: Mapped["LegacyStudy"] = relationship("LegacyStudy")

    experiment_type_id: Mapped[int] = mapped_column("EXPERIMENT_TYPE_ID", Integer)


class LegacyAnalysisJob(LegacyEMGBase):
    __tablename__ = "ANALYSIS_JOB"

    job_id: Mapped[int] = mapped_column("JOB_ID", Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column("PIPELINE_ID", Integer)
    analysis_status_id: Mapped[int] = mapped_column("ANALYSIS_STATUS_ID", Integer)
    result_directory: Mapped[str] = mapped_column("RESULT_DIRECTORY", String)
    external_run_ids: Mapped[str] = mapped_column("EXTERNAL_RUN_IDS", String)
    experiment_type_id: Mapped[int] = mapped_column("EXPERIMENT_TYPE_ID", Integer)
    secondary_accession: Mapped[str] = mapped_column("SECONDARY_ACCESSION", String)

    study_id: Mapped[int] = mapped_column("STUDY_ID", ForeignKey("STUDY.STUDY_ID"))
    study: Mapped["LegacyStudy"] = relationship(
        "LegacyStudy", back_populates="analysis_jobs"
    )

    sample_id: Mapped[int] = mapped_column("SAMPLE_ID", ForeignKey("SAMPLE.SAMPLE_ID"))
    sample: Mapped["LegacySample"] = relationship(
        "LegacySample", back_populates="analysis_jobs"
    )

    run_id: Mapped[int] = mapped_column("RUN_ID", ForeignKey("RUN.RUN_ID"))
    run: Mapped["LegacyRun"] = relationship("LegacyRun")

    downloads: Mapped[List["LegacyAnalysisJobDownload"]] = relationship(
        back_populates="analysis_job"
    )


class LegacyDownloadSubdir(LegacyEMGBase):
    __tablename__ = "DOWNLOAD_SUBDIR"

    id: Mapped[int] = mapped_column("SUBDIR_ID", Integer, primary_key=True)
    subdir: Mapped[str] = mapped_column("SUBDIR", String)


class LegacyDownloadDescription(LegacyEMGBase):
    __tablename__ = "DOWNLOAD_DESCRIPTION_LABEL"

    id: Mapped[int] = mapped_column("DESCRIPTION_ID", Integer, primary_key=True)
    description: Mapped[str] = mapped_column("DESCRIPTION", String)
    description_label: Mapped[str] = mapped_column("DESCRIPTION_LABEL", String)


class LegacyAnalysisJobDownload(LegacyEMGBase):
    __tablename__ = "ANALYSIS_JOB_DOWNLOAD"

    id: Mapped[int] = mapped_column("ID", Integer, primary_key=True)
    real_name: Mapped[str] = mapped_column("REAL_NAME", String)
    alias: Mapped[str] = mapped_column("ALIAS", String)

    format_id: Mapped[int] = mapped_column("FORMAT_ID", Integer)
    # no relationship implemented here but refers to LEGACY_FILE_FORMATS_MAP below

    group_id: Mapped[int] = mapped_column("GROUP_ID", Integer)
    # no relationship implemented here but refers to LEGACY_DOWNLOAD_TYPE_MAP below

    job_id: Mapped[int] = mapped_column("JOB_ID", ForeignKey("ANALYSIS_JOB.JOB_ID"))
    analysis_job: Mapped["LegacyAnalysisJob"] = relationship(
        "LegacyAnalysisJob", back_populates="downloads"
    )

    subdir_id: Mapped[int] = mapped_column(
        "SUBDIR_ID", ForeignKey("DOWNLOAD_SUBDIR.SUBDIR_ID")
    )
    subdir: Mapped["LegacyDownloadSubdir"] = relationship("LegacyDownloadSubdir")

    description_id: Mapped[int] = mapped_column(
        "DESCRIPTION_ID", ForeignKey("DOWNLOAD_DESCRIPTION_LABEL.DESCRIPTION_ID")
    )
    description: Mapped["LegacyDownloadDescription"] = relationship(
        "LegacyDownloadDescription"
    )


# Maps to convert legacy download metadata to new schema:

LEGACY_FILE_FORMATS_MAP = {
    1: DownloadFileType.TSV,
    2: DownloadFileType.TSV,
    3: DownloadFileType.CSV,
    4: DownloadFileType.FASTA,
    5: DownloadFileType.FASTA,
    6: DownloadFileType.BIOM,
    7: DownloadFileType.BIOM,
    8: DownloadFileType.BIOM,
    9: DownloadFileType.TREE,
    10: DownloadFileType.SVG,
}

LEGACY_DOWNLOAD_TYPE_MAP = {
    1: DownloadType.SEQUENCE_DATA,
    2: DownloadType.FUNCTIONAL_ANALYSIS,
    3: DownloadType.TAXONOMIC_ANALYSIS,
    4: DownloadType.TAXONOMIC_ANALYSIS,
    5: DownloadType.TAXONOMIC_ANALYSIS,
    6: DownloadType.STATISTICS,
    7: DownloadType.NON_CODING_RNAS,
    8: DownloadType.GENOME_ANALYSIS,
    9: DownloadType.GENOME_ANALYSIS,  # pangenome in legacy db
    # 10: unused,
    11: DownloadType.TAXONOMIC_ANALYSIS,
    12: DownloadType.TAXONOMIC_ANALYSIS,
    13: DownloadType.TAXONOMIC_ANALYSIS,
    14: DownloadType.FUNCTIONAL_ANALYSIS,
    15: DownloadType.TAXONOMIC_ANALYSIS,
    16: DownloadType.RO_CRATE,
}


@task(task_run_name="Get taxonomy for {mgya} from legacy mongo")
def get_taxonomy_from_api_v1_mongo(
    mgya: str,
) -> dict["analyses.models.Analysis.TaxonomySources", Optional[List]]:
    from analyses.models import Analysis  # prevent importing before apps ready

    logger = get_run_logger()
    mongo_dsn = str(settings.EMG_CONFIG.legacy_service.emg_mongo_dsn)
    mongo_client = pymongo.MongoClient(mongo_dsn)
    db = mongo_client[settings.EMG_CONFIG.legacy_service.emg_mongo_db]

    taxonomies_collection: Collection = db.analysis_job_taxonomy

    mgya_taxonomies = taxonomies_collection.find_one({"accession": mgya})

    if not mgya_taxonomies:
        logger.warning(f"Did not find {mgya} in legacy mongo")
        return {}

    return {
        Analysis.TaxonomySources.SSU.value: mgya_taxonomies.get("taxonomy_ssu"),
        Analysis.TaxonomySources.LSU.value: mgya_taxonomies.get("taxonomy_lsu"),
        Analysis.TaxonomySources.ITS_ONE_DB.value: mgya_taxonomies.get("itsonedb"),
        Analysis.TaxonomySources.UNITE.value: mgya_taxonomies.get("unite"),
        Analysis.TaxonomySources.PR2.value: None,  # not implemented in v5 pipeline
    }


legacy_emg_engine = create_engine(str(settings.EMG_CONFIG.legacy_service.emg_mysql_dsn))
LegacyEmgSession = sessionmaker(bind=legacy_emg_engine)


@contextmanager
def legacy_emg_db_session():
    """
    A SQLAlchemy session for the legacy EMG database
    (which was MySQL in production).
    This was the DB behind the v1 EMG API.

    Example:
        with legacy_emg_db_session() as session:
            study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == 100)
            legacy_study: LegacyStudy = session.scalar(study_select_stmt)
            ...
    """
    session = LegacyEmgSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
