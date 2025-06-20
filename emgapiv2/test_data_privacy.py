import pytest
from django.urls import reverse
from ninja_jwt.tokens import SlidingToken

from analyses.models import Analysis, Run, Study
from emgapiv2.api.auth import WebinJWTAuth
from ena.models import Study as ENAStudy


def create_analysis(is_private=False, is_ready=True):
    run = Run.objects.first()
    a = Analysis.objects.create(
        study=run.study,
        run=run,
        ena_study=run.ena_study,
        sample=run.sample,
        is_private=is_private,
    )
    if is_ready:
        a.status[a.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = True
        a.save()
    return a


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


@pytest.fixture
def webin_auth():
    return WebinJWTAuth()


@pytest.fixture
def webin_private_auth_token(webin_private_study):
    # Generate a token for the webin user
    token = SlidingToken()
    token["username"] = webin_private_study.webin_submitter
    return str(token)


@pytest.fixture
def auth_token_evil(webin_private_study):
    token = SlidingToken()
    token["username"] = webin_private_study.webin_submitter + "-evil"
    return str(token)


@pytest.mark.django_db
def test_superuser_can_list_private_studies(
    ninja_api_client, admin_user, webin_private_study
):
    response = ninja_api_client.get("/my-data/studies/", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == webin_private_study.accession


@pytest.mark.django_db
def test_webin_owner_can_list_private_studies(
    ninja_api_client, webin_private_study, webin_private_auth_token
):
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get("/my-data/studies/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == webin_private_study.accession


@pytest.mark.django_db
def test_wrong_webin_owner_cannot_list_anothers_private_studies(
    ninja_api_client, webin_private_study, webin_private_auth_token, auth_token_evil
):
    headers = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get("/my-data/studies/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["items"] == []


@pytest.mark.django_db
def test_bad_token_cannot_list_private_studies(ninja_api_client, webin_private_study):
    headers = {"Authorization": "Bearer invalid-token"}
    response = ninja_api_client.get("/my-data/studies/", headers=headers)
    assert response.status_code == 401


@pytest.mark.django_db
def test_public_studies_list_includes_no_private_data(
    webin_private_study,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
    raw_reads_mgnify_study,
):
    # Test as public user
    response = ninja_api_client.get("/studies/")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == raw_reads_mgnify_study.accession

    # Test as admin user
    response = ninja_api_client.get("/studies/", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == raw_reads_mgnify_study.accession

    # Test as webin owner
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get("/studies/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == raw_reads_mgnify_study.accession


@pytest.mark.django_db
def test_owner_can_view_private_study_detail(
    webin_private_study, ninja_api_client, webin_private_auth_token
):
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get(
        f"/studies/{webin_private_study.accession}", headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == webin_private_study.accession


@pytest.mark.django_db
def test_owner_can_view_private_study_analyses_list(
    private_analysis_with_download, ninja_api_client, webin_private_auth_token
):
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    study = private_analysis_with_download.study.accession
    response = ninja_api_client.get(f"/studies/{study}/analyses/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["items"][0]["accession"] == private_analysis_with_download.accession
    assert data["count"] == 1


@pytest.mark.django_db
def test_admin_can_view_private_study_detail(
    webin_private_study, ninja_api_client, admin_user
):
    response = ninja_api_client.get(
        f"/studies/{webin_private_study.accession}", user=admin_user
    )
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == webin_private_study.accession


@pytest.mark.django_db
def test_admin_can_view_private_study_analyses_list(
    private_analysis_with_download, ninja_api_client, admin_user
):
    study = private_analysis_with_download.study.accession
    response = ninja_api_client.get(f"/studies/{study}/analyses/", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    assert data["items"][0]["accession"] == private_analysis_with_download.accession
    assert data["count"] == 1


@pytest.mark.django_db
def test_wrong_owner_cannot_view_private_study_detail(
    webin_private_study, ninja_api_client, auth_token_evil
):
    headers = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get(
        f"/studies/{webin_private_study.accession}", headers=headers
    )
    assert response.status_code == 404
    data = response.json()
    assert "accession" not in data


@pytest.mark.django_db
def test_wrong_owner_cannot_view_private_study_analyses_list(
    private_analysis_with_download, ninja_api_client, auth_token_evil
):
    study = private_analysis_with_download.study.accession
    headers = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get(f"/studies/{study}/analyses/", headers=headers)
    assert response.status_code == 404


@pytest.mark.django_db
def test_samples_list_never_includes_private_sample(
    private_analysis_with_download,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
):
    private_sample = private_analysis_with_download.sample

    # Test as public user
    response = ninja_api_client.get("/samples/")
    assert response.status_code == 200
    data = response.json()
    sample_accessions = [sample["accession"] for sample in data["items"]]
    assert private_sample.first_accession not in sample_accessions

    # Test as admin user
    response = ninja_api_client.get("/samples/", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    sample_accessions = [sample["accession"] for sample in data["items"]]
    assert private_sample.first_accession not in sample_accessions

    # Test as owner
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get("/samples/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    sample_accessions = [sample["accession"] for sample in data["items"]]
    assert private_sample.first_accession not in sample_accessions


@pytest.mark.django_db
def test_private_sample_detail_access(
    private_analysis_with_download,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
    auth_token_evil,
):
    private_sample = private_analysis_with_download.sample
    sample_accession = private_sample.first_accession

    # Test as public user - should not be able to view
    response = ninja_api_client.get(f"/samples/{sample_accession}")
    assert response.status_code == 404

    # Test as evil user - should not be able to view
    headers_evil = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get(
        f"/samples/{sample_accession}", headers=headers_evil
    )
    assert response.status_code == 404

    # Test as admin user - should be able to view
    response = ninja_api_client.get(f"/samples/{sample_accession}", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == sample_accession

    # Test as owner - should be able to view
    headers_owner = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get(
        f"/samples/{sample_accession}", headers=headers_owner
    )
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == sample_accession


@pytest.mark.django_db
def test_analyses_list_never_includes_private_analysis(
    private_analysis_with_download,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
):
    private_analysis_accession = private_analysis_with_download.accession

    # Test as public user
    response = ninja_api_client.get("/analyses/")
    assert response.status_code == 200
    data = response.json()
    analysis_accessions = [analysis["accession"] for analysis in data["items"]]
    assert private_analysis_accession not in analysis_accessions

    # Test as admin user
    response = ninja_api_client.get("/analyses/", user=admin_user)
    assert response.status_code == 200
    data = response.json()
    analysis_accessions = [analysis["accession"] for analysis in data["items"]]
    assert private_analysis_accession not in analysis_accessions

    # Test as owner
    headers = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get("/analyses/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    analysis_accessions = [analysis["accession"] for analysis in data["items"]]
    assert private_analysis_accession not in analysis_accessions


@pytest.mark.django_db
def test_private_analysis_detail_access(
    private_analysis_with_download,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
    auth_token_evil,
):
    private_analysis_accession = private_analysis_with_download.accession

    # Test as public user - should not be able to view
    response = ninja_api_client.get(f"/analyses/{private_analysis_accession}")
    assert response.status_code == 404

    # Test as evil user - should not be able to view
    headers_evil = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}", headers=headers_evil
    )
    assert response.status_code == 404

    # Test as admin user - should be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}", user=admin_user
    )
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == private_analysis_accession

    # Test as owner - should be able to view
    headers_owner = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}", headers=headers_owner
    )
    assert response.status_code == 200
    data = response.json()
    assert data["accession"] == private_analysis_accession


@pytest.mark.django_db
def test_private_analysis_annotations_access(
    private_analysis_with_download,
    ninja_api_client,
    admin_user,
    webin_private_auth_token,
    auth_token_evil,
):
    private_analysis_accession = private_analysis_with_download.accession

    # Test /analyses/{accession}/annotations endpoint

    # Test as public user - should not be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations"
    )
    assert response.status_code == 404

    # Test as evil user - should not be able to view
    headers_evil = {"Authorization": f"Bearer {auth_token_evil}"}
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations", headers=headers_evil
    )
    assert response.status_code == 404

    # Test as admin user - should be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations", user=admin_user
    )
    assert response.status_code == 200

    # Test as owner - should be able to view
    headers_owner = {"Authorization": f"Bearer {webin_private_auth_token}"}
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations", headers=headers_owner
    )
    assert response.status_code == 200

    # Test /analyses/{accession}/annotations/{annotation_type} endpoint
    annotation_type = "pfams"

    # Test as public user - should not be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations/{annotation_type}"
    )
    assert response.status_code == 404

    # Test as evil user - should not be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations/{annotation_type}",
        headers=headers_evil,
    )
    assert response.status_code == 404

    # Test as admin user - should be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations/{annotation_type}",
        user=admin_user,
    )
    assert response.status_code == 200

    # Test as owner - should be able to view
    response = ninja_api_client.get(
        f"/analyses/{private_analysis_accession}/annotations/{annotation_type}",
        headers=headers_owner,
    )
    assert response.status_code == 200
