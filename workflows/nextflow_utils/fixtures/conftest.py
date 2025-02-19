import csv
import shutil
from datetime import datetime
from pathlib import Path
from textwrap import dedent as _

import pytest
from django.conf import settings


@pytest.fixture
def write_nextflow_tracefile():
    def _write(tracefile_path: Path) -> None:
        header = [
            "task_id",
            "hash",
            "native_id",
            "name",
            "status",
            "exit",
            "submit",
            "duration",
            "realtime",
            "%cpu",
            "peak_rss",
            "peak_vmem",
            "rchar",
            "wchar",
        ]

        data = [
            [
                1,
                "c4/1f6cf1",
                99732,
                "EBIMETAGENOMICS:ASSEMBLY_ANALYSIS_PIPELINE:RRNA_EXTRACTION:INFERNAL_CMSEARCH (test_assembly)",
                "CACHED",
                0,
                "2025-02-14 15:47:54.415",
                "3.1s",
                "3s",
                "101.4%",
                "172.8 MB",
                "939.8 MB",
                "11 MB",
                "26 KB",
            ],
            [
                2,
                "6d/ba8f1f",
                99734,
                "EBIMETAGENOMICS:ASSEMBLY_ANALYSIS_PIPELINE:RENAME_CONTIGS (test_assembly)",
                "CACHED",
                0,
                "2025-02-14 15:47:54.420",
                "909ms",
                "1s",
                "107.5%",
                "6.2 MB",
                "348.9 MB",
                "4.8 MB",
                "27.3 KB",
            ],
        ]

        tracefile_path.parent.mkdir(exist_ok=True, parents=True)

        with open(tracefile_path, mode="w", newline="") as file:
            writer = csv.writer(file, delimiter="\t")
            writer.writerow(header)
            writer.writerows(data)

    return _write


@pytest.fixture
def nextflow_hello_world_command():
    def _get_command(with_trace_flag: bool):
        command = "nextflow run hello -ansi-log false"

        workdir = Path(settings.EMG_CONFIG.slurm.default_workdir) / "hello-nextflow"

        shutil.rmtree(workdir, ignore_errors=True)
        workdir.mkdir(exist_ok=True)

        if with_trace_flag:
            command += " -with-trace trace-hello.txt"
        else:
            # This is what nf-core~like pipelines handle the trace files
            nf_config = workdir / "test-config.conf"
            trace_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            # Write the configuration to the file
            nf_config.write_text(
                _(
                    f"""
                trace {{
                    enabled = true
                    file    = "{workdir}/pipeline_info/execution_trace_{trace_timestamp}.txt"
                }}
                """
                )
            )

            command += f" -c {nf_config}"

        return command

    return _get_command
