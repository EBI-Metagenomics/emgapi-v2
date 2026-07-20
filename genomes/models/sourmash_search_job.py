from __future__ import annotations

import uuid

from django.db import models

from analyses.base_models.base_models import TimeStampedModel

from .genome_search_index import GenomeSearchIndex


class SourmashSearchJob(TimeStampedModel):
    class Status(models.TextChoices):
        RECEIVED = "RECEIVED", "Received"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        PARTIAL_SUCCESS = "PARTIAL_SUCCESS", "Partial success"
        FAILED = "FAILED", "Failed"
        NO_RESULTS = "NO_RESULTS", "No results"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.RECEIVED,
    )
    request_payload = models.JSONField(default=dict, blank=True)
    submitted_by = models.CharField(max_length=255, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    raw_results_archive_path = models.CharField(max_length=500, blank=True)
    error_summary = models.TextField(blank=True)

    class Meta:
        ordering = ("-created_at",)

    def __str__(self):
        return f"{self.id} ({self.status})"

    def recalculate_status(self):
        items = list(self.items.all())
        if not items:
            return self

        statuses = {item.status for item in items}
        started_values = [item.started_at for item in items if item.started_at]
        finished_values = [item.finished_at for item in items if item.finished_at]
        all_terminal = all(item.is_terminal for item in items)

        if SourmashSearchJobItem.Status.RUNNING in statuses:
            status = self.Status.RUNNING
        elif statuses.issubset(
            {
                SourmashSearchJobItem.Status.RECEIVED,
                SourmashSearchJobItem.Status.QUEUED,
            }
        ):
            status = self.Status.QUEUED
        elif any(
            item_status
            in {
                SourmashSearchJobItem.Status.RECEIVED,
                SourmashSearchJobItem.Status.QUEUED,
            }
            for item_status in statuses
        ):
            status = self.Status.RUNNING
        elif statuses == {SourmashSearchJobItem.Status.NO_RESULTS}:
            status = self.Status.NO_RESULTS
        elif statuses == {SourmashSearchJobItem.Status.SUCCESS}:
            status = self.Status.SUCCESS
        elif statuses == {SourmashSearchJobItem.Status.FAILED}:
            status = self.Status.FAILED
        elif SourmashSearchJobItem.Status.SUCCESS in statuses:
            status = self.Status.PARTIAL_SUCCESS
        elif statuses.issubset(
            {
                SourmashSearchJobItem.Status.FAILED,
                SourmashSearchJobItem.Status.NO_RESULTS,
            }
        ):
            status = self.Status.FAILED
        else:
            status = self.Status.RUNNING

        error_messages = [item.error_message for item in items if item.error_message]
        fields_to_update: list[str] = []
        new_started_at = min(started_values) if started_values else None
        new_finished_at = (
            max(finished_values) if all_terminal and finished_values else None
        )
        new_error_summary = "\n\n".join(error_messages[:10])

        for field_name, value in (
            ("status", status),
            ("started_at", new_started_at),
            ("finished_at", new_finished_at),
            ("error_summary", new_error_summary),
        ):
            if getattr(self, field_name) != value:
                setattr(self, field_name, value)
                fields_to_update.append(field_name)

        if fields_to_update:
            self.save(update_fields=fields_to_update)
        return self


class SourmashSearchJobItem(TimeStampedModel):
    class Status(models.TextChoices):
        RECEIVED = "RECEIVED", "Received"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        FAILED = "FAILED", "Failed"
        NO_RESULTS = "NO_RESULTS", "No results"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(
        SourmashSearchJob,
        on_delete=models.CASCADE,
        related_name="items",
    )
    search_index = models.ForeignKey(
        GenomeSearchIndex,
        on_delete=models.PROTECT,
        related_name="job_items",
    )
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.RECEIVED,
    )
    query_original_name = models.CharField(max_length=255)
    query_staged_path = models.CharField(max_length=500)
    task_result_id = models.CharField(max_length=64, blank=True)
    raw_csv_path = models.CharField(max_length=500, blank=True)
    result_summary = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ("created_at",)

    def __str__(self):
        return (
            f"{self.job_id}:{self.search_index.catalogue.catalogue_id}:"
            f"{self.query_original_name} ({self.status})"
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            self.Status.SUCCESS,
            self.Status.FAILED,
            self.Status.NO_RESULTS,
        }
