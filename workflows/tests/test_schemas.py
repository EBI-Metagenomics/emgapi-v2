from pathlib import Path


from workflows.data_io_utils.schemas.assembly import StudySummary, AssemblyStudySummary


class TestStudySummary:
    """Test the StudySummary schema."""

    def test_matches_file_with_valid_taxonomy_file(self):
        """Test that matches_file correctly identifies a taxonomy summary file."""
        summary_type = StudySummary(
            source="taxonomy",
            short_description="Test taxonomy",
            long_description="Test taxonomy description",
        )

        assert summary_type.matches_file(Path("PRJEB12345_taxonomy_study_summary.tsv"))

    def test_matches_file_with_valid_ko_file(self):
        """Test that matches_file correctly identifies a KO summary file."""
        summary_type = StudySummary(
            source="ko",
            short_description="Test KO",
            long_description="Test KO description",
        )

        assert summary_type.matches_file(Path("ERP106708_ko_study_summary.tsv"))

    def test_matches_file_rejects_wrong_source(self):
        """Test that matches_file rejects files with different source."""
        summary_type = StudySummary(
            source="taxonomy",
            short_description="Test",
            long_description="Test",
        )

        assert not summary_type.matches_file(Path("PRJEB12345_ko_study_summary.tsv"))

    def test_matches_file_rejects_missing_suffix(self):
        """Test that matches_file rejects files without proper suffix."""
        summary_type = StudySummary(
            source="taxonomy",
            short_description="Test",
            long_description="Test",
        )

        assert not summary_type.matches_file(Path("PRJEB12345_taxonomy.tsv"))
        assert not summary_type.matches_file(Path("PRJEB12345_taxonomy_summary.tsv"))

    def test_matches_file_rejects_no_accession(self):
        """Test that matches_file rejects files without study accession."""
        summary_type = StudySummary(
            source="taxonomy",
            short_description="Test",
            long_description="Test",
        )

        assert not summary_type.matches_file(Path("taxonomy_study_summary.tsv"))

    def test_matches_file_with_path_object(self):
        """Test that matches_file works with Path objects."""
        summary_type = StudySummary(
            source="pfam",
            short_description="Test",
            long_description="Test",
        )

        path = Path("/some/directory/PRJEB12345_pfam_study_summary.tsv")
        assert summary_type.matches_file(path)

    def test_schema_has_required_fields(self):
        """Test that StudySummary has all required fields."""
        summary_type = StudySummary(
            source="test",
            short_description="Short desc",
            long_description="Long desc",
        )

        assert summary_type.source == "test"
        assert summary_type.short_description == "Short desc"
        assert summary_type.long_description == "Long desc"


class TestAssemblyStudySummary:
    """Test the AssemblyStudySummary registry."""

    def test_all_types_returns_correct_count_and_instances(self):
        """Test that all_types returns 8 StudySummary instances."""
        all_types = AssemblyStudySummary.all_types()

        assert len(all_types) == 8
        for summary_type in all_types:
            assert isinstance(summary_type, StudySummary)
            assert summary_type.source
            assert summary_type.short_description
            assert summary_type.long_description

    def test_all_sources_are_unique(self):
        """Test that all summary types have unique source identifiers."""
        all_types = AssemblyStudySummary.all_types()
        sources = [t.source for t in all_types]

        assert len(sources) == len(set(sources))

    def test_summary_types_match_expected_files(self):
        """Test that each summary type matches its expected file pattern."""
        expected_sources = [
            "taxonomy",
            "ko",
            "antismash",
            "go",
            "goslim",
            "pfam",
            "interpro",
            "kegg_modules",
        ]

        for source in expected_sources:
            summary_type = next(
                (t for t in AssemblyStudySummary.all_types() if t.source == source),
                None,
            )
            assert summary_type is not None, f"Missing summary type for {source}"
            assert summary_type.matches_file(
                Path(f"PRJEB12345_{source}_study_summary.tsv")
            )
