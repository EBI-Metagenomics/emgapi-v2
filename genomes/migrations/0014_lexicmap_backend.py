from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("genomes", "0013_alter_cataloguegenome_result_directory_and_more")]

    operations = [
        migrations.AlterField(
            model_name="genomesearchindex",
            name="backend",
            field=models.CharField(
                max_length=32,
                choices=[
                    ("sourmash", "Sourmash"),
                    ("branchwater", "Branchwater"),
                    ("lexicmap", "LexicMap"),
                ],
            ),
        ),
    ]
