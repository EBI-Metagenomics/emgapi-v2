import pytest
from django.urls import reverse

from analyses.models import Analysis, Run, Study
from ena.models import Study as ENAStudy


def create_analysis(is_private=False):
    run = Run.objects.first()
    return Analysis.objects.create(
        study=run.study,
        run=run,
        ena_study=run.ena_study,
        sample=run.sample,
        is_private=is_private,
    )


@pytest.mark.django_db(transaction=True)
def test_public_api_studies_endpoint(ninja_api_client):
    """Test that public API only returns public studies"""
    public_study = Study.objects.create(title="Public Study", is_private=False)
    Study.objects.create(title="Private Study", is_private=True)
    response = ninja_api_client.get("/studies/")
    # assert response.status_code == status.HTTP_200_OK
    assert response.status_code == 200
    assert len(response.json()["items"]) == 1  # Only public study

    assert response.json()["items"][0]["accession"] == public_study.accession


@pytest.mark.django_db(transaction=True)
def test_public_api_analyses_endpoint(raw_read_run, ninja_api_client):
    """Test that public API only returns public analyses"""
    public_analysis = create_analysis(is_private=False)
    create_analysis(is_private=True)

    response = ninja_api_client.get("/analyses/")

    # assert response.status_code == status.HTTP_200_OK
    assert response.status_code == 200
    # Only public analysis should be returned
    assert len(response.json()["items"]) == 1
    assert response.json()["items"][0]["accession"] == public_analysis.accession


@pytest.mark.django_db(transaction=True)
def test_admin_view_studies(admin_client):
    """Test that admin interface shows all studies"""
    public_study = Study.objects.create(title="Public Study", is_private=False)
    private_study = Study.objects.create(title="Private Study", is_private=True)

    url = reverse("admin:analyses_study_changelist")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode("utf-8")
    assert public_study.title in content
    assert private_study.title in content


@pytest.mark.django_db(transaction=True)
def test_admin_view_analyses(raw_read_run, admin_client):
    """Test that admin interface shows all analyses"""
    public_analysis = create_analysis(is_private=False)
    private_analysis = create_analysis(is_private=True)

    url = reverse("admin:analyses_analysis_changelist")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode("utf-8")
    assert public_analysis.accession in content
    assert private_analysis.accession in content


@pytest.mark.django_db(transaction=True)
def test_study_manager_methods():
    """Test various manager methods for privacy handling"""
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    public_study = Study.objects.create(
        ena_study=ena_study, title="Public Study", is_private=False
    )
    private_study = Study.objects.create(title="Private Study", is_private=True)

    assert Study.public_objects.count() == 1
    assert Study.public_objects.first() == public_study
    assert Study.objects.count() == 2
    private_studies = Study.public_objects.private_only()
    # a bit odd naming, public_objects is really "privacy controlled objects but by default public"
    assert private_studies.count() == 1
    assert private_studies.first() == private_study


@pytest.mark.django_db(transaction=True)
def test_suppressed_study_manager_methods():
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    public_study = Study.objects.create(
        ena_study=ena_study, title="Public Study", is_private=False
    )
    private_study = Study.objects.create(title="Private Study", is_private=True)

    assert Study.public_objects.count() == 1
    assert Study.objects.count() == 2

    public_study.is_suppressed = True
    private_study.is_suppressed = True
    public_study.save()
    private_study.save()
    assert Study.public_objects.count() == 0
    assert Study.objects.count() == 2


@pytest.mark.django_db(transaction=True)
def test_suppressed_study_propagates_to_analyses(raw_read_run):
    public_analysis = create_analysis(is_private=False)
    assert public_analysis.study.is_suppressed is False
    assert Analysis.objects.count() == 1
    public_analysis.study.is_suppressed = True
    public_analysis.study.save()
    # should have triggered analysis to be suppressed
    public_analysis.refresh_from_db()
    assert public_analysis.study.is_suppressed

    assert Analysis.public_objects.count() == 0
    assert Analysis.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_manager_analysis_methods(raw_read_run):
    public_analysis = create_analysis(is_private=False)
    private_analysis = create_analysis(is_private=True)

    assert Analysis.public_objects.count() == 1
    assert Analysis.public_objects.first() == public_analysis
    assert Analysis.objects.count() == 2
    private_analyses = Analysis.public_objects.private_only()
    assert private_analyses.count() == 1
    assert private_analyses.first() == private_analysis

    public_analysis.annotations = {"test": "data"}
    public_analysis.save()

    analysis = Analysis.public_objects_and_annotations.first()
    assert analysis.annotations == {"test": "data"}
    all_analyses = Analysis.objects_and_annotations.all()
    assert len(all_analyses) == 2
