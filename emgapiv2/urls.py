"""emgapiv2 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import include, path
from django.conf import settings

from analyses.admin.study import (
    jump_to_latest_study_admin,
    jump_to_watched_studies_admin,
)

from .api import api

BASE_URL = settings.BASE_URL

urlpatterns = [
    path(
        f"{BASE_URL}admin/latest_study",
        jump_to_latest_study_admin,
        name="admin_jump_latest_study",
    ),
    path(
        f"{BASE_URL}admin/watched_studies",
        jump_to_watched_studies_admin,
        name="admin_jump_watched_studies",
    ),
    path(f"{BASE_URL}admin/", admin.site.urls),
    path(f"{BASE_URL}__debug__/", include("debug_toolbar.urls")),
    path("fieldfiles/", include("db_file_storage.urls")),
    path(BASE_URL, api.urls),
    path(f"{BASE_URL}workflows/", include("workflows.urls")),
]
admin.site.index_title = "EMG DB Administration"
