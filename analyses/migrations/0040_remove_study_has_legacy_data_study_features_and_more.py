# Generated by Django 5.1.6 on 2025-04-09 10:32

import analyses.models
import django.contrib.postgres.indexes
import emgapiv2.model_utils
from django.conf import settings
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0039_remove_run_is_ready_remove_sample_is_ready_and_more"),
        ("ena", "0004_alter_sample_additional_accessions_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.RemoveField(
            model_name="study",
            name="has_legacy_data",
        ),
        migrations.AddField(
            model_name="study",
            name="features",
            field=emgapiv2.model_utils.JSONFieldWithSchema(
                default=analyses.models.Study.StudyFeatures,
                encoder=emgapiv2.model_utils._PydanticEncoder,
                is_list=False,
                schema=analyses.models.Study.StudyFeatures,
                strict=False,
            ),
        ),
        migrations.AddIndex(
            model_name="study",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["features"], name="analyses_st_feature_f239f9_gin"
            ),
        ),
    ]
