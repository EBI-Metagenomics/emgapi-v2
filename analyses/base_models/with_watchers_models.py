from django.contrib.auth.models import User
from django.db import models


class WithWatchersModel(models.Model):
    """
    Abstract model that introduces `watchers`, i.e. Users who are watching the concrete model that inherits this.
    Intended for handling notifications etc.
    """

    watchers = models.ManyToManyField(
        User,
        related_name="studies_watching",
        blank=True,
        help_text="Users who will get notifications for this object",
    )

    class Meta:
        abstract = True
