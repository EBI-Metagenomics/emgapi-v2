import unittest
import subprocess
from workflows.flows.import_v5_biomes import import_biomes_flow

class TestImportBiomesFlow(unittest.TestCase):
    def test_import_biomes_flow(self):
        # Define a test URL
        # test_url = "http://localhost:8004/v1/biomes"
        test_url = "https://www.ebi.ac.uk/metagenomics/api/v1/biomes"

        # Run the flow
        try:
            import_biomes_flow(test_url)
        except subprocess.CalledProcessError as e:
            self.fail(f"Flow execution failed with error: {e}")

if __name__ == "__main__":
    unittest.main()
# import unittest
# from unittest.mock import patch
# from workflows.flows.import_v5_biomes import import_biomes_flow
#
# class TestImportBiomesFlow(unittest.TestCase):
#     @patch("workflows.flows.import_v5_biomes.subprocess.run")
#     def test_import_biomes_flow(self, mock_subprocess_run):
#         # Mock the subprocess.run to prevent actual execution
#         mock_subprocess_run.return_value = None
#
#         # Call the flow with a test URL
#         test_url = "https://api.test.com/biomes"
#         import_biomes_flow(test_url)
#
#         # Assert that subprocess.run was called with the correct arguments
#         mock_subprocess_run.assert_called_once_with(
#             ["python", "manage.py", "import_biomes_from_api_v1", "--url", test_url],
#             check=True
#         )
#
# if __name__ == "__main__":
#     unittest.main()