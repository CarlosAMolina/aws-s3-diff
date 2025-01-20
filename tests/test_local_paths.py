import unittest
from pathlib import Path

from src.local_paths import LocalPaths


class TestLocalPaths(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._current_path = Path(__file__).parent.absolute()

    def test_config_directory_has_expected_value(self):
        self.assertEqual(self._current_path.parent.joinpath("config"), LocalPaths().config_directory)

    def test_all_results_directory_has_expected_value(self):
        self.assertEqual(self._current_path.parent.joinpath("s3-results"), LocalPaths().all_results_directory)

    def test_analysis_date_time_file_has_expected_value(self):
        self.assertEqual(
            self._current_path.parent.joinpath("s3-results/analysis_date_time.txt"),
            LocalPaths().analysis_date_time_file,
        )
