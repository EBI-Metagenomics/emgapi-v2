from django.core.management.base import BaseCommand
from analyses.models import Study
from analyses.base_models.with_experiment_type_models import WithExperimentTypeModel


class Command(BaseCommand):
    help = "Update the download_group for all Study downloads to include the pipeline version and type."

    def handle(self, *args, **options):
        # Filter studies with non-empty downloads list. In Django JSONField, we can exclude []
        studies = Study.objects.exclude(downloads=[])
        count = 0
        for study in studies:
            analyses = study.analyses.all()
            if not analyses.exists():
                self.stdout.write(
                    self.style.WARNING(
                        f"Study {study.accession} has no analyses. Skipping."
                    )
                )
                continue

            # Infer the experiment type based on the analyses
            experiment_types = set(analyses.values_list("experiment_type", flat=True))

            inferred_type = None
            if all(
                et == WithExperimentTypeModel.ExperimentTypes.AMPLICON
                for et in experiment_types
            ):
                inferred_type = "amplicon"
            elif all(
                et
                in [
                    WithExperimentTypeModel.ExperimentTypes.ASSEMBLY,
                    WithExperimentTypeModel.ExperimentTypes.HYBRID_ASSEMBLY,
                    WithExperimentTypeModel.ExperimentTypes.LONG_READ_ASSEMBLY,
                ]
                for et in experiment_types
            ):
                inferred_type = "assembly"
            elif any(
                et
                in [
                    WithExperimentTypeModel.ExperimentTypes.METAGENOMIC,
                    WithExperimentTypeModel.ExperimentTypes.METATRANSCRIPTOMIC,
                ]
                for et in experiment_types
            ):
                inferred_type = "rawreads"

            if not inferred_type:
                self.stdout.write(
                    self.style.WARNING(
                        f"Could not infer experiment type for study {study.accession} with types {experiment_types}. Skipping."
                    )
                )
                continue

            updated = False
            version = "v6"

            for download in study.downloads:
                current_group = download.get("download_group", "")
                if (
                    current_group.startswith("study_summary")
                    and f".{version}." not in current_group
                ):
                    # Replace "study_summary" with "study_summary.v6.<inferred_type>"
                    # If current_group was "study_summary.suffix", it becomes "study_summary.v6.type.suffix"
                    # If current_group was "study_summary", it becomes "study_summary.v6.type"
                    if current_group == "study_summary":
                        new_group = f"study_summary.{version}.{inferred_type}"
                    else:
                        new_group = current_group.replace(
                            "study_summary.",
                            f"study_summary.{version}.{inferred_type}.",
                        )

                    download["download_group"] = new_group
                    updated = True

            if updated:
                study.save()
                count += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Updated study {study.accession} with inferred type '{inferred_type}'."
                    )
                )

        self.stdout.write(self.style.SUCCESS(f"Successfully updated {count} studies."))
