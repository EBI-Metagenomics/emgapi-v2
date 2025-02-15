import os


__all__ = ["UNSET", "TemporaryEnv"]


class UnsetEnvVarType:
    pass


UNSET = UnsetEnvVarType()


class TemporaryEnv:
    """
    Context manager for temporarily assigning env vars.
    Usage example:

    with TemporaryEnv(USERNAME='reinhardt', PASSWORD='guitar', OPTIONAL=UNSET):
        some_library_depending_on_env_vars_in_env()
        assert "OPTIONAL" not in os.environ
    """

    def __init__(self, **kwargs):
        self.temporary_env_vars = {k: v for k, v in kwargs.items() if not v == UNSET}

    def __enter__(self):
        self.previous_values = {
            key: os.environ.get(key, "") for key in self.temporary_env_vars.keys()
        }
        os.environ.update(self.temporary_env_vars)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        os.environ.update(self.previous_values)
