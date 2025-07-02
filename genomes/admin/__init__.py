import importlib
import pkgutil

# Import any admin files from this dir
for module_info in pkgutil.iter_modules(__path__):
    if not module_info.name.startswith("_"):
        importlib.import_module(f"{__name__}.{module_info.name}")
