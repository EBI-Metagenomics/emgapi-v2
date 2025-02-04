import enum

from django.utils.version import PY311

# Define StrEnum for Python3.10, to work like 3.11+
if PY311:  # or later
    FutureStrEnum = enum.StrEnum
else:

    class FutureStrEnum(str, enum.Enum):
        def __str__(self):
            return str(self.value)


class DjangoChoicesCompatibleStrEnum(FutureStrEnum):
    # TODO: after python 3.11 adopted everywhere switch str, Enum -> StrEnum
    """
    This is a str Enum that can also be easily converted to a Django Choices object.
    """

    @classmethod
    def as_choices(cls):
        return [(item.value, item.name) for item in cls]

    @classmethod
    def get_member_by_value(cls, value: str) -> "DjangoChoicesCompatibleStrEnum":
        for item in cls:
            if item.value == value:
                return item
