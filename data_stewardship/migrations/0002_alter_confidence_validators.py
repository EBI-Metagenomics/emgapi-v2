import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("data_stewardship", "0001_initial")]

    operations = [
        migrations.AlterField(
            model_name="biomesamplebiomeprediction",
            name="confidence",
            field=models.FloatField(
                blank=True,
                null=True,
                validators=[
                    django.core.validators.MinValueValidator(0),
                    django.core.validators.MaxValueValidator(1),
                ],
            ),
        ),
        migrations.AlterField(
            model_name="biomestudybiomeprediction",
            name="confidence",
            field=models.FloatField(
                blank=True,
                null=True,
                validators=[
                    django.core.validators.MinValueValidator(0),
                    django.core.validators.MaxValueValidator(1),
                ],
            ),
        ),
    ]
