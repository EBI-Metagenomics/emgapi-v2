from typing import Dict

from pydantic import RootModel


class CogCategories(RootModel):
    root: Dict[str, str]


class KeggClasses(RootModel):
    root: Dict[str, str]
