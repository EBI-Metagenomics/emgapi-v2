import logging
from typing import Union

from django.http import HttpRequest
from ninja_extra import permissions, ControllerBase
from ninja_extra.exceptions import NotFound
from ninja_extra.permissions import IsAdminUser, BasePermission, AsyncBasePermission

from analyses.base_models.base_models import VisibilityControlledModel

logger = logging.getLogger(__name__)


class IsPublic(permissions.BasePermission):
    """
    Note that this should only be used on endpoints that call the self.get_object_or_exception method,
    since this perm is designed for object checks.
    """

    def has_permission(self, request: HttpRequest, controller) -> bool:
        return True

    def has_object_permission(
        self, request: HttpRequest, controller, obj: VisibilityControlledModel
    ) -> bool:
        logger.debug(f"IsPublic permission for {obj}? {not obj.is_private}")
        return not obj.is_private


class IsWebinOwner(IsPublic):
    """
    Note that this should only be used on endpoints that call the self.get_object_or_exception method,
    since this perm is designed for object checks.
    """

    def has_object_permission(
        self, request: HttpRequest, controller, obj: VisibilityControlledModel
    ) -> bool:
        if not request.user:
            return False
        logger.debug(
            f"IsWebinOwner permission for {obj}? {obj.webin_submitter == request.user.id} since {obj.webin_submitter} ~ {request.user.id}"
        )
        return obj.webin_submitter == request.user.id


class IsAdminUserWithObjectPerms(IsAdminUser):
    """
    The built-in IsAdminUser does not check object permissions. This does.
    """

    def has_object_permission(
        self, request: HttpRequest, controller, obj: VisibilityControlledModel
    ) -> bool:
        return self.has_permission(request, controller)


class UnauthorisedIsUnfoundController(ControllerBase):
    @classmethod
    def permission_denied(
        cls, permission: Union[BasePermission, AsyncBasePermission]
    ) -> None:
        """
        If an object permission fails (e.g. user is not logged in as the owning webin), return a 404 instead of 403.
        This avoids data existence leakage.
        """
        raise NotFound()
