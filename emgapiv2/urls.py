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

from analyses.admin.study import (
    jump_to_latest_study_admin,
    jump_to_watched_studies_admin,
)

from .api import api

urlpatterns = [
    path(
        "admin/latest_study", jump_to_latest_study_admin, name="admin_jump_latest_study"
    ),
    path(
        "admin/watched_studies",
        jump_to_watched_studies_admin,
        name="admin_jump_watched_studies",
    ),
    path("admin/", admin.site.urls),
    path("api/v2/", api.urls),
    path("__debug__/", include("debug_toolbar.urls")),
    path("workflows/", include("workflows.urls")),
]
admin.site.index_title = "EMG DB Administration"
