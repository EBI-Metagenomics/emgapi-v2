from pathlib import Path

from lxml import etree
from pydantic_xml import BaseXmlModel, attr, element, wrapped

XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
SCHEMA_LOCATION = "https://www.ebi.ac.uk/ebisearch/XML4dbDumps.xsd"
PROJECT_DATABASE_NAME = "EMG_Project"
RUN_DATABASE_NAME = "EMG_run"


class EBISearchDate(BaseXmlModel, tag="date"):
    type: str = attr()
    value: str = attr()


class EBISearchField(BaseXmlModel, tag="field"):
    name: str = attr()
    value: str = ""


class EBISearchHierarchy(BaseXmlModel, tag="hierarchical_field"):
    name: str = attr()
    root: str = element()
    children: list[str] = element(tag="child", default_factory=list)


class EBISearchReference(BaseXmlModel, tag="ref"):
    dbkey: str = attr()
    dbname: str = attr()


class EBISearchEntry(BaseXmlModel, tag="entry"):
    identifier: str = attr(name="id")
    name: str | None = element(default=None)
    description: str | None = element(default=None)
    dates: list[EBISearchDate] | None = wrapped("dates", entity=element(), default=None)
    additional_fields: list[EBISearchField | EBISearchHierarchy] | None = wrapped(
        "additional_fields", entity=element(), default=None
    )
    cross_references: list[EBISearchReference] | None = wrapped(
        "cross_references", entity=element(), default=None
    )


class EBISearchDatabase(BaseXmlModel, tag="database", nsmap={"xsi": XSI_NAMESPACE}):
    schema_location: str = attr(
        name=f"{{{XSI_NAMESPACE}}}noNamespaceSchemaLocation",
        default=SCHEMA_LOCATION,
    )
    name: str = element()
    description: str | None = element(default=None)
    release: str | None = element(default=None)
    entry_count: int | None = element(default=None)
    entries: list[EBISearchEntry] = wrapped(
        "entries", entity=element(), default_factory=list
    )


def value(value_: object) -> str:
    return "" if value_ is None else str(value_)


def field(name: str, value_: object) -> EBISearchField:
    return EBISearchField(name=name, value=value(value_))


def reference(database: str, identifier: object) -> EBISearchReference:
    return EBISearchReference(dbkey=value(identifier), dbname=database)


def hierarchy(name: str, values: list[str]) -> EBISearchHierarchy:
    return EBISearchHierarchy(
        name=name,
        root=value(values[0]),
        children=[value(item) for item in values[1:]],
    )


def database(
    name: str,
    description: str | None = None,
    release: str | None = None,
    count: int | None = None,
) -> EBISearchDatabase:
    return EBISearchDatabase(
        name=name,
        description=description,
        release=release,
        entry_count=count,
    )


def write_xml(path: Path, database_: EBISearchDatabase) -> None:
    root = database_.to_xml_tree(exclude_none=True)
    etree.indent(root, space="    ")
    etree.ElementTree(root).write(str(path), encoding="utf-8", pretty_print=True)


def write_deletions(path: Path, database_name: str, entry_ids: list[str]) -> None:
    database_ = database(database_name)
    database_.entries.extend(
        EBISearchEntry(identifier=entry_id) for entry_id in entry_ids
    )
    write_xml(path, database_)
