import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent

CHECK = """
import importlib, pkgutil, sys
import proteins.accession as package
for module in pkgutil.iter_modules(package.__path__):
    if not module.name.startswith("__"):  # __main__ would run the command line
        importlib.import_module(f"proteins.accession.{module.name}")
allowed = (str(package.__path__[0]), sys.argv[1] + "/proteins/__init__.py")
print(*sorted(
    name for name, m in sys.modules.items()
    if name == "django"
    or (getattr(m, "__file__", None) or "").startswith(sys.argv[1])
    and not m.__file__.startswith(allowed)
))
"""


def test_accession_imports_nothing_from_django_or_the_rest_of_emgapi():
    # mgyp-accession's image holds only proteins/__init__.py and proteins/accession/.
    result = subprocess.run(
        [sys.executable, "-c", CHECK, str(REPO)],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.split() == []
