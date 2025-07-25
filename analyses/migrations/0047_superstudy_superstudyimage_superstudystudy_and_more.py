# Generated by Django 5.2.1 on 2025-07-14 16:05

import db_file_storage.storage
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0046_alter_study_watchers"),
    ]

    operations = [
        migrations.CreateModel(
            name="SuperStudy",
            fields=[
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "slug",
                    models.SlugField(primary_key=True, serialize=False, unique=True),
                ),
                ("title", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True)),
                (
                    "logo",
                    models.ImageField(
                        blank=True,
                        null=True,
                        storage=db_file_storage.storage.DatabaseFileStorage(
                            base_url="fieldfiles/"
                        ),
                        upload_to="analyses.SuperStudyImage/bytes/filename/mimetype",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "super studies",
            },
        ),
        migrations.CreateModel(
            name="SuperStudyImage",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("bytes", models.BinaryField()),
                ("filename", models.CharField(max_length=255)),
                ("mimetype", models.CharField(max_length=50)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="SuperStudyStudy",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("is_flagship", models.BooleanField(default=True)),
                (
                    "study",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="analyses.study"
                    ),
                ),
                (
                    "super_study",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="analyses.superstudy",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "super study studies",
                "unique_together": {("study", "super_study")},
            },
        ),
        migrations.AddField(
            model_name="superstudy",
            name="studies",
            field=models.ManyToManyField(
                related_name="super_studies",
                through="analyses.SuperStudyStudy",
                to="analyses.study",
            ),
        ),
    ]
