from activate_django_first import EMG_CONFIG

from analyses.models import Analysis


PIPELINE_CONFIGS_BY_EXPERIMENT_TYPE = {
    Analysis.ExperimentTypes.AMPLICON: EMG_CONFIG.amplicon_pipeline,
    Analysis.ExperimentTypes.METAGENOMIC: EMG_CONFIG.rawreads_pipeline,
    Analysis.ExperimentTypes.METATRANSCRIPTOMIC: EMG_CONFIG.rawreads_pipeline,
    Analysis.ExperimentTypes.ASSEMBLY: EMG_CONFIG.assembly_analysis_pipeline,
    Analysis.ExperimentTypes.HYBRID_ASSEMBLY: EMG_CONFIG.assembly_analysis_pipeline,
    Analysis.ExperimentTypes.LONG_READ_ASSEMBLY: EMG_CONFIG.assembly_analysis_pipeline,
}


def get_pipeline_config_for_experiment_type(
    experiment_type: Analysis.ExperimentTypes | str,
):
    normalized_experiment_type = (
        experiment_type
        if isinstance(experiment_type, Analysis.ExperimentTypes)
        else Analysis.ExperimentTypes(experiment_type)
    )
    return PIPELINE_CONFIGS_BY_EXPERIMENT_TYPE[normalized_experiment_type]


def get_current_pipeline_version_for_experiment_type(
    experiment_type: Analysis.ExperimentTypes | str,
) -> Analysis.PipelineVersions:
    pipeline_config = get_pipeline_config_for_experiment_type(experiment_type)
    return Analysis.pipeline_version_from_config(pipeline_config.pipeline_version)


def get_v6_family_pipeline_versions_for_experiment_type(
    experiment_type: Analysis.ExperimentTypes | str,
) -> tuple[Analysis.PipelineVersions, ...]:
    normalized_experiment_type = (
        experiment_type
        if isinstance(experiment_type, Analysis.ExperimentTypes)
        else Analysis.ExperimentTypes(experiment_type)
    )
    if normalized_experiment_type == Analysis.ExperimentTypes.AMPLICON:
        return (Analysis.PipelineVersions.v6, Analysis.PipelineVersions.v6_1)
    return (Analysis.PipelineVersions.v6,)
