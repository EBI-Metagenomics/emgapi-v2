# Generated by Django 5.2 on 2025-04-29 08:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0041_analysis_external_results_dir_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="sample",
            name="metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
