"""Tests for serialized LexicMap match locations."""

import unittest

from genomes.lexicmap_schema import LexicMapMatch


class LexicMapLocationTest(unittest.TestCase):
    def test_serialized_locus(self):
        """Keep browser coordinates ascending on either strand."""
        for start, end, strand in ((21, 100, "+"), (100, 21, "-")):
            with self.subTest(strand=strand):
                match = LexicMapMatch(
                    genome="MGYG000000003",
                    catalogue="ocean-prokaryotes",
                    query_coverage=100,
                    identity=100,
                    bitscore=145,
                    evalue=2.93e-40,
                    sequence_id="MGYG000000003_1",
                    query_start=1,
                    query_end=80,
                    subject_start=start,
                    subject_end=end,
                    strand=strand,
                )
                result = match.model_dump()
                self.assertEqual(result["locus"], "MGYG000000003_1:21-100")
                self.assertEqual(result["strand"], strand)
                self.assertEqual(
                    LexicMapMatch.model_validate(result).model_dump(), result
                )
