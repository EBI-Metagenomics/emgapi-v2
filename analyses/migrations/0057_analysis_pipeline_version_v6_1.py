from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0056_samplerelatedsample_sample_related_samples_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="analysis",
            name="pipeline_version",
            field=models.CharField(
                choices=[
                    ("V1", "v1.0"),
                    ("V2", "v2.0"),
                    ("V3", "v3.0"),
                    ("V4", "v4.0"),
                    ("V4.1", "v4.1"),
                    ("V5", "v5.0"),
                    ("V6", "v6.0"),
                    ("V6.1", "v6.1"),
                ],
                default="V6",
                max_length=5,
            ),
        ),
    ]
