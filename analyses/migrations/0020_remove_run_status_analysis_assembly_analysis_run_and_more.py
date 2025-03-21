# Generated by Django 5.0.4 on 2024-08-02 11:32

import django.db.models.deletion
from django.db import migrations, models

import analyses.models


class Migration(migrations.Migration):
    dependencies = [
        ("analyses", "0019_analysis_pipeline_version"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="run",
            name="status",
        ),
        migrations.AddField(
            model_name="analysis",
            name="assembly",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="analyses",
                to="analyses.assembly",
            ),
        ),
        migrations.AddField(
            model_name="analysis",
            name="run",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="analyses",
                to="analyses.run",
            ),
        ),
        migrations.AddField(
            model_name="analysis",
            name="status",
            field=models.JSONField(
                blank=True,
                default=analyses.models.Analysis.AnalysisStates.default_status,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="analysis",
            name="study",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="analyses",
                to="analyses.study",
                to_field="accession",
            ),
        ),
    ]
