import pytest

import analyses.models
import ena.models
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)


@pytest.mark.django_db
def test_analyses_of_private_study_are_private(
    prefect_harness, raw_reads_mgnify_study, mgnify_assemblies
):
    analyses.models.Assembly.objects.filter(
        id__in=[assembly.id for assembly in mgnify_assemblies]
    ).update(assembly_study=raw_reads_mgnify_study)
    ena.models.Study.objects.filter(pk=raw_reads_mgnify_study.ena_study.pk).update(
        is_private=True, webin_submitter="Webin-1"
    )

    created_analyses = create_analyses_for_assemblies(
        raw_reads_mgnify_study.id,
        [assembly.first_accession for assembly in mgnify_assemblies],
    )

    assert len(created_analyses) == len(mgnify_assemblies)
    for analysis in created_analyses:
        assert analysis.is_private
        assert analysis.webin_submitter == "Webin-1"
