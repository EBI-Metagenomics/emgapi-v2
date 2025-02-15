import pytest

import analyses.models
import ena.models


@pytest.mark.django_db(transaction=True)
def test_ena_suppression_and_privacy_propagation(mgnify_assemblies, raw_read_analyses):
    assert analyses.models.Study.objects.count() == 1

    assert ena.models.Study.objects.count() == 1

    ena_study: ena.models.Study = ena.models.Study.objects.first()

    assert not analyses.models.Study.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Analysis.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=True).exists()

    ena_study.is_suppressed = True
    ena_study.save()

    # everything derived should be suppressed
    assert analyses.models.Study.all_objects.filter(is_suppressed=True).exists()
    assert analyses.models.Sample.all_objects.filter(is_suppressed=True).exists()
    assert analyses.models.Run.all_objects.filter(is_suppressed=True).exists()
    assert analyses.models.Analysis.all_objects.filter(is_suppressed=True).exists()
    assert analyses.models.Assembly.all_objects.filter(is_suppressed=True).exists()

    assert not analyses.models.Study.all_objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Sample.all_objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Run.all_objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Analysis.all_objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Assembly.all_objects.filter(is_suppressed=False).exists()

    ena_study.is_suppressed = False
    ena_study.save()
    # everything should be unsuppressed now
    assert not analyses.models.Study.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Sample.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Run.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Analysis.all_objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=True).exists()

    # everything should have been public so far
    assert analyses.models.Study.objects.count() == 1
    assert analyses.models.Study.all_objects.count() == 1
    assert not analyses.models.Study.all_objects.filter(is_private=True).exists()
    assert not analyses.models.Sample.all_objects.filter(is_private=True).exists()
    assert not analyses.models.Run.all_objects.filter(is_private=True).exists()
    assert not analyses.models.Analysis.all_objects.filter(is_private=True).exists()
    assert not analyses.models.Assembly.all_objects.filter(is_private=True).exists()

    ena_study.is_private = True
    ena_study.save()

    # everything derived should be private now
    assert analyses.models.Study.all_objects.filter(is_private=True).exists()
    assert analyses.models.Sample.all_objects.filter(is_private=True).exists()
    assert analyses.models.Run.all_objects.filter(is_private=True).exists()
    assert analyses.models.Analysis.all_objects.filter(is_private=True).exists()
    assert analyses.models.Assembly.all_objects.filter(is_private=True).exists()

    assert not analyses.models.Study.all_objects.filter(is_private=False).exists()
    assert not analyses.models.Sample.all_objects.filter(is_private=False).exists()
    assert not analyses.models.Run.all_objects.filter(is_private=False).exists()
    assert not analyses.models.Analysis.all_objects.filter(is_private=False).exists()
    assert not analyses.models.Assembly.all_objects.filter(is_private=False).exists()

    assert analyses.models.Study.objects.count() == 0
    assert analyses.models.Study.all_objects.count() == 1
