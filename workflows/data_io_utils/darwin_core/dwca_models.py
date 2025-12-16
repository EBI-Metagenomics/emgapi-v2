from __future__ import annotations

from datetime import datetime, date
from typing import List, Optional

from pydantic import EmailStr
from pydantic_xml import BaseXmlModel, attr, element, wrapped

EML_NS = "eml://ecoinformatics.org/eml-2.1.1"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
# GBIF GMP 1.1 schema
GBIF_GMP_XSD = "http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd"


# EML required:
# -------------
# dataset/title
# creator
# metadataProvider
# pubDate
# language
# abstract
# contact
# intellectualRights
# distribution
# methods
# project/title


# ---------- Meta ----------
class IndividualName(BaseXmlModel, tag="individualName"):
    givenName: Optional[str] = element(default=None)
    surName: Optional[str] = element(default=None)


class Address(BaseXmlModel, tag="address"):
    deliveryPoint: Optional[str] = element(default=None)
    city: Optional[str] = element(default=None)
    administrativeArea: Optional[str] = element(default=None)
    postalCode: Optional[str] = element(default=None)
    country: Optional[str] = element(default=None)


class UserId(BaseXmlModel, tag="userId"):
    directory: Optional[str] = attr(default=None)  # e.g., "ORCID"
    value: str = wrapped("userId", default="")  # element text content


class ResponsibleParty(BaseXmlModel, tag="party"):
    individualName: Optional[IndividualName] = element(default=None)
    organizationName: Optional[str] = element(default=None)  # usually all we need
    positionName: Optional[str] = element(default=None)
    address: Optional[Address] = element(default=None)
    electronicMailAddress: Optional[EmailStr] = element(default=None)
    phone: Optional[str] = element(default=None)
    onlineUrl: Optional[str] = element(default=None)
    userId: Optional[List[UserId]] = element(default=None)


class Contact(ResponsibleParty, tag="contact"): ...


class MetadataProvider(ResponsibleParty, tag="metadataProvider"): ...


class Creator(ResponsibleParty, tag="creator"): ...


# ---------- Dataset substructures ----------
# class Para(BaseXmlModel, tag="para"):
#     text: str = wrapped("para")


class Abstract(BaseXmlModel, tag="abstract"):
    para: List[str] = element(tag="para", default_factory=list)


class KeywordSet(BaseXmlModel, tag="keywordSet"):
    keyword: List[str] = element(default_factory=list)
    keywordThesaurus: Optional[str] = element(default=None)


class License(BaseXmlModel, tag="license"):
    url: Optional[str] = attr(default=None)
    value: str = wrapped("license", default="")  # e.g., "CC-BY-4.0"


class IntellectualRights(BaseXmlModel, tag="intellectualRights"):
    # Machine-readable license per GMP 1.1
    para: Optional[str] = element(tag="para", default=None)
    license: Optional[License] = element(default=None)


class BoundingCoordinates(BaseXmlModel, tag="boundingCoordinates"):
    westBoundingCoordinate: float = element()
    eastBoundingCoordinate: float = element()
    northBoundingCoordinate: float = element()
    southBoundingCoordinate: float = element()


class GeographicCoverage(BaseXmlModel, tag="geographicCoverage"):
    geographicDescription: Optional[str] = element(default=None)
    boundingCoordinates: Optional[BoundingCoordinates] = element(default=None)


class Taxon(BaseXmlModel, tag="taxon"):
    taxonRankName: Optional[str] = element(default=None)
    taxonRankValue: Optional[str] = element(default=None)
    commonName: Optional[List[str]] = element(default=None)


class TaxonomicCoverage(BaseXmlModel, tag="taxonomicCoverage"):
    generalTaxonomicCoverage: Optional[str] = element(default=None)
    taxon: Optional[List[Taxon]] = element(default=None)


class SingleDateTime(BaseXmlModel, tag="singleDateTime"):
    calendarDate: date = element()


class RangeOfDates(BaseXmlModel, tag="rangeOfDates"):
    beginDate: SingleDateTime = element()
    endDate: SingleDateTime = element()


class TemporalCoverage(BaseXmlModel, tag="temporalCoverage"):
    rangeOfDates: Optional[List[RangeOfDates]] = element(default=None)
    singleDateTime: Optional[List[SingleDateTime]] = element(default=None)


class Coverage(BaseXmlModel, tag="coverage"):
    geographicCoverage: Optional[List[GeographicCoverage]] = element(default=None)
    taxonomicCoverage: Optional[List[TaxonomicCoverage]] = element(default=None)
    temporalCoverage: Optional[List[TemporalCoverage]] = element(default=None)


class ProjectPersonnel(BaseXmlModel, tag="personnel"):
    role: Optional[str] = element(default=None)  # e.g., principalInvestigator
    individualName: Optional[IndividualName] = element(default=None)
    organizationName: Optional[str] = element(default=None)
    electronicMailAddress: Optional[EmailStr] = element(default=None)
    userId: Optional[List[UserId]] = element(default=None)


class StudyAreaDescription(BaseXmlModel, tag="studyAreaDescription"):
    descriptor: Optional[str] = element(default=None)
    descriptorValue: Optional[str] = element(default=None)


class Project(BaseXmlModel, tag="project"):
    title: Optional[str] = element(default=None)
    personnel: Optional[List[ProjectPersonnel]] = element(default=None)
    funding: Optional[str] = element(default=None)
    studyAreaDescription: Optional[StudyAreaDescription] = element(default=None)
    designDescription: Optional[str] = element(default=None)
    projectId: Optional[str] = element(default=None)


class MethodStep(BaseXmlModel, tag="methodStep"):
    description: Optional[Abstract] = element(default=None)
    qualityControl: Optional[str] = element(default=None)


class Sampling(BaseXmlModel, tag="sampling"):
    studyExtent: Optional[Abstract] = element(default=None)
    samplingDescription: Optional[Abstract] = element(default=None)


class Methods(BaseXmlModel, tag="methods"):
    methodStep: Optional[List[MethodStep]] = element(
        default=None
    )  # e.g. pipeline version
    sampling: Optional[Sampling] = element(
        default=None
    )  # e.g. the study summary as this is aggregate over dataset


class Maintenance(BaseXmlModel, tag="maintenance"):
    description: Optional[Abstract] = element(default=None)
    maintenanceUpdateFrequency: Optional[str] = element(default=None)  # e.g., "monthly"


class DataFormat(BaseXmlModel, tag="dataFormat"):
    class TextFormat(BaseXmlModel, tag="textFormat"):
        numHeaderLines: Optional[int] = element(default=None)
        recordDelimiter: Optional[str] = element(default=None)
        attributeOrientation: Optional[str] = element(default=None)

        class SimpleDelimited(BaseXmlModel, tag="simpleDelimited"):
            fieldDelimiter: str = element()

        simpleDelimited: Optional[SimpleDelimited] = element(default=None)

    textFormat: Optional[TextFormat] = element(default=None)


class DistributionOnline(BaseXmlModel, tag="online"):
    url: str = element()


class Distribution(BaseXmlModel, tag="distribution"):
    online: Optional[DistributionOnline] = element(default=None)


# class DataTable(BaseXmlModel, tag="dataTable"):
#     entityName: Optional[str] = element(default=None)
#     entityDescription: Optional[str] = element(default=None)
#     physical: Optional[Physical] = element(default=None)


class Citation(BaseXmlModel, tag="citation"):  # TODO: should have an identifier?
    bibliographicCitation: Optional[str] = element(default=None)


# ---------- Dataset ----------
class Dataset(BaseXmlModel, tag="dataset"):
    title: str = element()  # e.g. study title
    creator: List[ResponsibleParty] = element(
        default_factory=list
    )  # e.g. submission centre
    metadataProvider: List[ResponsibleParty] = element(
        default_factory=list
    )  # e.g. submission centre
    contact: List[ResponsibleParty] = element(
        default_factory=list
    )  # e.g. submission centre

    pubDate: Optional[datetime] = element(default=None)  # today
    language: Optional[str] = element(
        default=None
    )  # ISO 639-1/GBIF enumeration recommended. e.g. en

    abstract: Abstract = element()  # e.g. study description
    keywordSet: Optional[KeywordSet] = element(default=None)  # optional

    intellectualRights: IntellectualRights = element()  # e.g. link to ebi TOS
    coverage: Optional[Coverage] = element(default=None)  # optional
    project: Optional[Project] = element(default=None)  # e.g. study, just title needed
    methods: Optional[Methods] = element(
        default=None
    )  #  e.g. study description and piepline
    maintenance: Optional[Maintenance] = element(
        default=None
    )  # optional (update frequency)
    distribution: Optional[Distribution] = element(default=None)

    # dataTable: Optional[List[DataTable]] = element(default=None)  # ?
    citation: Optional[Citation] = element(
        default=None
    )  # e.g. a publication associated with study

    # Additional recommended fields can be added here as needed


# ---------- GBIF Additional Metadata ----------
class Bibliography(BaseXmlModel, tag="bibliography"):
    citation: Optional[list[Citation]] = element(default=None)


class GbifAdditionalMetadata(BaseXmlModel, tag="gbif"):
    bibliography: Optional[Bibliography] = element(default=None)


class AdditionalMetadata(BaseXmlModel, tag="additionalMetadata"):
    gbif: Optional[GbifAdditionalMetadata] = element(default=None)


# ---------- Root eml ----------
class Eml(BaseXmlModel, tag="eml", ns=EML_NS):
    # Root attributes
    system: Optional[str] = attr(default=None)
    scope: Optional[str] = attr(default="document")
    packageId: str = attr()
    xsi_schemaLocation: str = attr(
        name="{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
    )

    dataset: Dataset = element()

    additionalMetadata: Optional[AdditionalMetadata] = element(default=None)

    class Config:
        # Register namespaces to ensure prefixes in output  # TODO: do we need GBIF?
        ns_map = {
            "eml": EML_NS,
            "xsi": XSI_NS,
        }


# def make_minimal_example() -> Eml:
#     # Required abstract
#     abstract = Abstract(para=[Para(text="Occurrence records across Cambridge wetlands.")])
#
#     # Required parties
#     creator = ResponsibleParty(
#         individualName=IndividualName(givenName="Alex", surName="Rogers"),
#         organizationName="EMBL-EBI",
#         electronicMailAddress="sandyr@ebi.ac.uk",
#         userId=[UserId(directory="ORCID", value="https://orcid.org/0000-0002-4283-6135")],
#     )
#     metadata_provider = ResponsibleParty(
#         individualName=IndividualName(givenName="Alex", surName="Rogers"),
#         organizationName="EMBL-EBI",
#         electronicMailAddress="sandyr@ebi.ac.uk",
#     )
#     contact = ResponsibleParty(
#         individualName=IndividualName(givenName="Data", surName="Manager"),
#         organizationName="EMBL-EBI",
#         electronicMailAddress="data@ebi.ac.uk",
#     )
#
#     # Machine-readable license
#     rights = IntellectualRights(
#         para=Para(text="Creative Commons Attribution 4.0 International (CC BY 4.0)."),
#         license=License(url="https://creativecommons.org/licenses/by/4.0/legalcode", value="CC-BY-4.0"),
#     )
#
#     dataset = Dataset(
#         title="Example Occurrence Dataset for Cambridge Wetlands",
#         creator=[creator],
#         metadataProvider=[metadata_provider],
#         contact=[contact],
#         pubDate=date.today(),
#         language="en",
#         abstract=abstract,
#         intellectualRights=rights,
#     )
#
#     eml = Eml(
#         system="GBIF-IPT",
#         scope="document",
#         packageId="619a4b95-1a82-4006-be6a-7dbe3c9b33c5/eml-1.xml",
#         xsi_schemaLocation=f"{EML_NS} {GBIF_GMP_XSD}",
#         dataset=dataset,
#     )
#     return eml
#
#
# if __name__ == "__main__":
#     eml = make_minimal_example()
#     xml_bytes = eml.to_xml(encoding="utf-8", xml_declaration=True, pretty_print=True)
#     print(xml_bytes.decode("utf-8"))
