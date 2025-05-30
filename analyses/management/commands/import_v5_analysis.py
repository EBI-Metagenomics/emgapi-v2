import glob
import logging
import time

import numpy as np
import pandas as pd
import requests
from django.core.management.base import BaseCommand, CommandError
from requests import JSONDecodeError

from analyses.models import AnalysedContig, Analysis, Sample, Study
from ena import models as ena_models

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Imports (legacy) v5 analyses into the DB – study at a time."
    assembly_accession_to_existing_analysis_accession = {}
    assembly_accession_to_sample_accession = {}

    existing_study_accession: str = None
    ena_study: ena_models.Study = None
    study: Study = None

    def add_arguments(self, parser):
        parser.add_argument(
            "-d",
            "--study_dir",
            type=str,
            help="File path to study directory",
            required=True,
        )

    def handle(self, *args, **options):
        study_directory = options.get("study_dir")
        self.process_study_directory(study_directory)

    def get_analyses_for_study(self, study_directory):
        study_accession = study_directory.split("/")[-2]
        print(f"Study accession is {study_accession}")
        # e.g. 2022/04/ERP1/version5.0 -> ERP1

        page_url = (
            f"https://www.ebi.ac.uk/metagenomics/api/v1/studies/{study_accession}"
            f"/analyses?experiment_type=assembly&pipeline_version=5.0"
        )

        while page_url:
            print(f"Fetching from {page_url}")
            version_5_analyses_response = requests.get(page_url)
            try:
                assert version_5_analyses_response.status_code == 200
                version_5_analyses = version_5_analyses_response.json()
                if not self.existing_study_accession:
                    self.existing_study_accession = version_5_analyses["data"][0][
                        "relationships"
                    ]["study"]["data"]["id"]
                page_url = version_5_analyses["links"]["next"]
            except (AssertionError, JSONDecodeError, KeyError) as e:
                logger.error(e)
                raise CommandError(
                    f"Could not retrieve analyses for study {study_accession}"
                )
            for analysis in version_5_analyses["data"]:
                assembly_accession = analysis["relationships"]["assembly"]["data"]["id"]
                analysis_accession = analysis["id"]
                sample_accession = analysis["relationships"]["sample"]["data"]["id"]
                self.assembly_accession_to_existing_analysis_accession[
                    assembly_accession
                ] = analysis_accession
                self.assembly_accession_to_sample_accession[assembly_accession] = (
                    sample_accession
                )
            if page_url:
                print("Throttling API calls for 1s...")
                time.sleep(1)

    def process_study_directory(self, study_directory):
        ena_study_accession = study_directory.split("/")[-2]
        self.get_analyses_for_study(study_directory)
        self.ena_study, _ = ena_models.Study.objects.get_or_create(
            accession=ena_study_accession, defaults={"title": ena_study_accession}
        )
        self.study, _ = Study.objects.get_or_create(
            id=int(self.existing_study_accession.lstrip("MGYS")),
            defaults={
                "title": self.existing_study_accession,
                "ena_study": self.ena_study,
            },
        )

        erz_prefixed_dirs = glob.glob(f"{study_directory}/ERZ*")
        for erz_prefixed_dir in erz_prefixed_dirs:
            self.process_erz_prefixed_dir(erz_prefixed_dir)

    def process_erz_prefixed_dir(self, erz_prefixed_dir):
        print(f"Handling erz_prefixed_dir {erz_prefixed_dir}")
        chunked_dirs = glob.glob(f"{erz_prefixed_dir}/*")
        for chunked_dir in chunked_dirs:
            self.process_chunked_dir(chunked_dir)

    def process_chunked_dir(self, chunked_dir):
        print(f"Handling chunked_dir {chunked_dir}")
        analysed_assembly_dirs = glob.glob(f"{chunked_dir}/ERZ*")
        for analysed_assembly_dir in analysed_assembly_dirs:
            self.process_analysed_assembly_dir(analysed_assembly_dir)

    def process_analysed_assembly_dir(self, analysed_assembly_dir):
        print(f"Handling analysed_assembly_dir {analysed_assembly_dir}")
        assembly_accession = analysed_assembly_dir.split("/")[-1].split("_")[0]
        try:
            existing_analysis_accession = (
                self.assembly_accession_to_existing_analysis_accession[
                    assembly_accession
                ]
            )
        except KeyError:
            logger.warning(
                f"Could not find existing analysis accession {assembly_accession}"
            )
        else:
            print(f"Found existing analysis accession {existing_analysis_accession}")
            self.process_existing_analysis(
                existing_analysis_accession, analysed_assembly_dir, assembly_accession
            )

    def process_existing_analysis(
        self, existing_analysis_accession, analysed_assembly_dir, assembly_accession
    ):
        analysis_id = int(str(existing_analysis_accession).lstrip("MGYA"))
        ena_sample, _ = ena_models.Sample.objects.get_or_create(
            accession=self.assembly_accession_to_sample_accession[assembly_accession],
            defaults={"study": self.ena_study},
        )
        sample, _ = Sample.objects.get_or_create(
            ena_sample=ena_sample, ena_study=self.ena_study
        )
        analysis, _ = Analysis.objects.get_or_create(
            id=analysis_id,
            defaults={
                "study": self.study,
                "results_dir": analysed_assembly_dir,
                "sample": sample,
                "ena_study": self.ena_study,
                "pipeline_version": Analysis.PipelineVersions.v5,
            },
        )
        self.process_functional_annotations(analysed_assembly_dir, analysis)
        self.process_contigs(analysed_assembly_dir, analysis)
        analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = True

    def process_functional_annotations(self, analysed_assembly_dir, analysis):
        print(f"Processing functional annotations in {analysed_assembly_dir}")
        functional_annotations = glob.glob(
            f"{analysed_assembly_dir}/functional-annotation/*.summary.*"
        )
        for functional_annotation in functional_annotations:
            print(f"Processing functional annotation for {functional_annotation}")
            if "ips" in functional_annotation:
                analysis.annotations[Analysis.INTERPRO_IDENTIFIERS] = (
                    self.ingest_functional_annotations(
                        functional_annotation,
                        ["count", "ipr", "description"],
                        accession_col="ipr",
                    )
                )
            elif "go_slim" in functional_annotation:
                analysis.annotations[Analysis.GO_SLIMS] = (
                    self.ingest_functional_annotations(
                        functional_annotation,
                        ["go", "description", "category", "count"],
                        accession_col="go",
                    )
                )
            elif "go" in functional_annotation:
                analysis.annotations[Analysis.GO_TERMS] = (
                    self.ingest_functional_annotations(
                        functional_annotation,
                        ["go", "description", "category", "count"],
                        accession_col="go",
                    )
                )
            elif "ko" in functional_annotation:
                analysis.annotations[Analysis.KEGG_ORTHOLOGS] = (
                    self.ingest_functional_annotations(
                        functional_annotation,
                        ["count", "ko", "description"],
                        accession_col="ko",
                    )
                )
            #     TODO kegg modules
            elif "pfam" in functional_annotation:
                analysis.annotations[Analysis.PFAMS] = (
                    self.ingest_functional_annotations(
                        functional_annotation,
                        ["count", "pfam", "description"],
                        accession_col="pfam",
                    )
                )
        analysis.save()

    def ingest_functional_annotations(
        self,
        functional_annotation_file: str,
        columns: [str],
        accession_col: str,
        csv_sep: str = ",",
        count_col="count",
    ):
        annos = pd.read_csv(
            functional_annotation_file,
            sep=csv_sep,
            names=columns,
            index_col=columns.index(accession_col),
            dtype={count_col: int},
        )
        return (
            annos.sort_values(by=count_col, ascending=False)
            .replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
        )

    @staticmethod
    def _parse_gff_attributes(gff_attributes: str) -> dict:
        possible_annotation_lists_in_attrs = [
            "kegg",
            "cog",
            "pfam",
            "interpro",
            "go",
        ]  # case sensitive

        sections: [str] = gff_attributes.split(";")
        sections = filter(
            lambda section: any(
                map(section.startswith, possible_annotation_lists_in_attrs)
            ),
            sections,
        )
        attr_tuples = [tuple(section.split("=")) for section in sections]
        return {
            attr_name: attr_value_string.split(",")
            for attr_name, attr_value_string in attr_tuples
        }

    def process_contigs(self, analysed_assembly_dir, analysis):
        try:
            fasta_index_file = glob.glob(f"{analysed_assembly_dir}/*.fai")[0]
        except IndexError:
            logger.warning(f"Could not find fasta index for {analysed_assembly_dir}")
            fasta_index = None
        else:
            fasta_index = pd.read_csv(
                fasta_index_file,
                sep="\t",
                names=["seqid", "length", "start", "bases", "bytes"],
                index_col="seqid",
            )
            fasta_index.drop(columns=["start", "bases", "bytes"], inplace=True)

        try:
            annotation_gff = glob.glob(
                f"{analysed_assembly_dir}/functional-annotation/*annotations.gff.bgz"
            )[0]
        except IndexError:
            logger.warning(f"No annotation gff in {analysed_assembly_dir}")
            return

        print(f"Processing annotation GFF for {annotation_gff}")
        gff = pd.read_csv(
            annotation_gff,
            sep="\t",
            compression="gzip",
            names=[
                "seqid",
                "source",
                "annotation_type",
                "start",
                "end",
                "score",
                "strand",
                "phase",
                "attributes",
            ],
            comment="#",
        )

        gff["parsed_attributes"] = gff.attributes.apply(self._parse_gff_attributes)

        parsed_gff = pd.concat(  # expand to columns for each annotation annotation_type
            [
                gff.drop(columns="parsed_attributes"),
                gff.parsed_attributes.apply(pd.Series),
            ],
            axis=1,
        )
        parsed_gff = parsed_gff.join(fasta_index, on="seqid", how="left")
        parsed_gff.rename(
            columns={
                "kegg": AnalysedContig.KEGGS,
                "cog": AnalysedContig.COGS,
                "pfam": AnalysedContig.PFAMS,
                "interpro": AnalysedContig.INTERPROS,
                "go": AnalysedContig.GOS,
            },
            inplace=True,
        )

        print(f"There are {len(gff)} annotations")
        contigs = parsed_gff.groupby("seqid")
        print(f"There are {contigs.ngroups} contigs")

        contigs_chunk_to_insert = []

        for contig_name, contig_regions in contigs:
            annotations = {
                anno_type: contig_regions.explode(anno_type)[anno_type]
                .dropna()
                .unique()
                .tolist()
                for anno_type in [
                    AnalysedContig.KEGGS,
                    AnalysedContig.COGS,
                    AnalysedContig.PFAMS,
                    AnalysedContig.INTERPROS,
                    AnalysedContig.GOS,
                ]
            }
            contig = AnalysedContig(
                contig_id=contig_name,
                analysis=analysis,
                length=contig_regions.length.iloc[0],
                coverage=0,
                #     TODO coverage
                annotations=annotations,
            )
            contigs_chunk_to_insert.append(contig)
            if len(contigs_chunk_to_insert) >= 100:
                print(f"Inserting {len(contigs_chunk_to_insert)} contigs")
                AnalysedContig.objects.bulk_create(contigs_chunk_to_insert)
                contigs_chunk_to_insert = []
