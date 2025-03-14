import contextlib
import os

__all__ = ["chdir"]


@contextlib.contextmanager
def future_chdir(path):
    """Temporarily change the working directory within a context. For python <3.11"""
    original_directory = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original_directory)


if hasattr(contextlib, "chdir"):
    chdir = contextlib.chdir
else:
    chdir = future_chdir
