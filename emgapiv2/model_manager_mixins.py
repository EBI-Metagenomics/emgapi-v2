class SuppressionFilterManagerMixin:
    """Filter suppressed rows from a cooperatively composed model manager."""

    def get_queryset(self):
        return super().get_queryset().filter(is_suppressed=False)
