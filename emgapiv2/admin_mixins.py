"""Shared admin actions for Webin accounts."""

from urllib.parse import quote, urlencode

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from unfold.decorators import action

from emgapiv2.webin_tokens import create_webin_token


class ImpersonateOwnerAdminMixin:
    """Open an object in the web client with its owner's Webin token."""

    impersonation_path: str
    impersonation_accession_field = "accession"

    def has_impersonate_owner_permission(
        self, request: HttpRequest, object_id: str | None = None
    ) -> bool:
        """Allow active staff accounts without an additional model permission."""
        return self.admin_site.has_permission(request)

    @action(
        description="Impersonate owner",
        permissions=["impersonate_owner"],
        icon="person",
    )
    def impersonate_owner(self, request: HttpRequest, object_id: str) -> HttpResponse:
        """Issue a Webin owner token for the selected object."""
        if not self.has_impersonate_owner_permission(request, object_id):
            raise PermissionDenied
        obj = get_object_or_404(self.get_queryset(request), pk=object_id)
        account = getattr(
            obj, "webin_submitter", settings.EMG_CONFIG.webin.emg_webin_account
        )
        if not account:
            self.message_user(
                request, "No Webin account is available for this object.", level="error"
            )
            return redirect(
                reverse(
                    f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_change",
                    args=[obj.pk],
                )
            )
        accession = getattr(obj, self.impersonation_accession_field)
        root = settings.EMG_CONFIG.service_urls.web_client_root.rstrip("/")

        token = create_webin_token(account)
        self.log_change(request, obj, f"Issued a web client token for {account}.")
        fragment = urlencode({"impersonation_token": str(token)})
        response = redirect(
            f"{root}/{self.impersonation_path}/{quote(str(accession), safe='')}#{fragment}"
        )
        response["Cache-Control"] = "no-store"
        response["Referrer-Policy"] = "no-referrer"
        return response
