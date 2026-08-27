from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("analyses", "0058_assembly_idx_assembly_ena_acc_gin_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="run",
            name="instrument_model",
        ),
        migrations.RemoveField(
            model_name="run",
            name="instrument_platform",
        ),
    ]
