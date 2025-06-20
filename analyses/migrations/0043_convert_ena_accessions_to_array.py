from django.db import migrations


def convert_jsonfield_to_array(apps, schema_editor):
    """
    Convert the JSONField ena_accessions to a format compatible with ArrayField.
    This is needed because PostgreSQL cannot directly cast JSONB to character varying array.
    """
    # Get the models that have ena_accessions field
    Assembly = apps.get_model("analyses", "Assembly")
    Run = apps.get_model("analyses", "Run")
    Sample = apps.get_model("analyses", "Sample")
    Study = apps.get_model("analyses", "Study")

    # Process each model
    for model in [Assembly, Run, Sample, Study]:
        # Get all instances of the model
        for instance in model.objects.all():
            # Ensure ena_accessions is a list of strings
            if instance.ena_accessions is None:
                instance.ena_accessions = []
            elif not isinstance(instance.ena_accessions, list):
                # If it's not a list, convert it to a list
                instance.ena_accessions = [str(instance.ena_accessions)]
            else:
                # Ensure all elements are strings
                instance.ena_accessions = [str(acc) for acc in instance.ena_accessions]

            # Save the instance with the converted data
            instance.save(update_fields=["ena_accessions"])


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0042_sample_metadata"),
    ]

    operations = [
        migrations.RunPython(convert_jsonfield_to_array, migrations.RunPython.noop),
    ]
