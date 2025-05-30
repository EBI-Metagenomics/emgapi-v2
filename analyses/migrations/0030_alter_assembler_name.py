# Generated by Django 5.1.6 on 2025-02-26 12:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0029_analysis_is_suppressed"),
    ]

    operations = [
        migrations.AlterField(
            model_name="assembler",
            name="name",
            field=models.CharField(
                blank=True,
                choices=[
                    ("metaspades", "MetaSPAdes"),
                    ("megahit", "MEGAHIT"),
                    ("spades", "SPAdes"),
                    ("flye", "Flye"),
                ],
                max_length=20,
                null=True,
            ),
        ),
    ]
