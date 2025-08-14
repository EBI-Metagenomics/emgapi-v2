from django.contrib import admin
from unfold.admin import ModelAdmin
from unfold.decorators import display

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.slurm_status import SlurmStatus


@admin.register(OrchestratedClusterJob)
class OrchestratedClusterJobAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["id", "cluster_job_id", "flow_run_id", "job_submit_description"]
    list_filter = [
        "last_known_state",
        "updated_at",
        "created_at",
    ]
    list_display = ["id", "name", "cluster_job_id", "display_status"]
    readonly_fields = [
        "id",
        "created_at",
        "updated_at",
        "nextflow_trace",
        "cluster_log",
    ]
    ordering = ["-updated_at"]

    @display(
        description="Last known status",
        label={
            SlurmStatus.failed: "danger",
            SlurmStatus.running: "warning",
            SlurmStatus.completed: "success",
            SlurmStatus.terminated: "danger",
            SlurmStatus.unknown: "info",
            SlurmStatus.pending: "warning",
            SlurmStatus.stopped: "danger",
            SlurmStatus.out_of_memory: "danger",
            SlurmStatus.suspended: "warning",
            SlurmStatus.completing: "success",
        },
    )
    def display_status(self, instance: OrchestratedClusterJob):
        return instance.last_known_state

    fieldsets = (
        (
            None,
            {
                "fields": [
                    "id",
                    "created_at",
                    "updated_at",
                    "cluster_job_id",
                    "flow_run_id",
                ]
            },
        ),
        (
            "Job submission",
            {
                "classes": ["tab"],
                "fields": [
                    "job_submit_description",
                    "input_files_hashes",
                ],
            },
        ),
        (
            "State",
            {
                "classes": ["tab"],
                "fields": [
                    "last_known_state",
                    "state_checked_at",
                ],
            },
        ),
        (
            "Logs",
            {
                "classes": ["tab"],
                "fields": [
                    "cluster_log",
                    "nextflow_trace",
                ],
            },
        ),
    )
