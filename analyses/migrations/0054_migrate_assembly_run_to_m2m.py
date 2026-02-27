from django.db import migrations


def copy_run_to_m2m(apps, schema_editor):
    Assembly = apps.get_model("analyses", "Assembly")
    for assembly in Assembly.objects.all():
        if assembly.run:
            assembly.runs.add(assembly.run)


def reverse_copy_run_to_m2m(apps, schema_editor):
    Assembly = apps.get_model("analyses", "Assembly")
    for assembly in Assembly.objects.all():
        assembly.run = assembly.runs.first()
        assembly.save()


class Migration(migrations.Migration):

    dependencies = [
        ("analyses", "0053_add_assembly_runs_m2m_fields"),
    ]

    operations = [
        migrations.RunPython(copy_run_to_m2m, reverse_copy_run_to_m2m),
    ]
