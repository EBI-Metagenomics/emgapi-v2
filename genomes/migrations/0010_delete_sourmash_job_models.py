from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("genomes", "0009_genomesearchindex_sourmashsearchjob_and_more"),
    ]

    operations = [
        migrations.DeleteModel(
            name="SourmashSearchJobItem",
        ),
        migrations.DeleteModel(
            name="SourmashSearchJob",
        ),
    ]
