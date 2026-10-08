"""Run the real-binary smoke test with manage.py test in the search worker."""

import gzip
import random
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from django.conf import settings

from genomes.lexicmap import search
from genomes.lexicmap_schema import SearchQuery


class LexicMapSmokeTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("lexicmap"), "Run in the search worker image")
    def test_index_and_search(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sequence = "".join(random.Random(17).choices("ACGT", k=5000))
            for accession, opener, suffix in (
                ("MGYG000000001", open, ".fna"),
                ("MGYG000000002", gzip.open, ".fna.gz"),
            ):
                with opener(root / f"{accession}{suffix}", "wt") as handle:
                    handle.write(f">contig\n{sequence}\n")
            subprocess.run(
                [
                    "lexicmap",
                    "index",
                    "-I",
                    str(root),
                    "-O",
                    str(root / "index.lmi"),
                    "-j",
                    "2",
                    "-c",
                    "2",
                ],
                check=True,
            )
            with patch.object(settings.EMG_CONFIG.lexicmap, "index_root", str(root)):
                for length in (50, 500):
                    results = search(
                        SearchQuery(sequence=sequence[500 : 500 + length]),
                        [{"catalogue": "test", "path": "index.lmi"}],
                    )
                    self.assertEqual(
                        {r["genome"] for r in results},
                        {"MGYG000000001", "MGYG000000002"},
                    )
                    self.assertTrue(
                        all(
                            r["identity"] == 100 and r["query_coverage"] == 100
                            for r in results
                        )
                    )
