class BranchwaterError(Exception):
    """A controlled Branchwater failure that is safe to translate at an API boundary."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
