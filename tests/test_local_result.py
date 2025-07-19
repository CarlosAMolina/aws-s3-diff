import unittest
from pathlib import Path

from aws_s3_diff.local_result import LocalPath


class TestLocalPath(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._current_path = Path(__file__).parent.absolute()

    def test_config_directory_has_expected_value(self):
        self.assertEqual(self._current_path.parent.joinpath("config"), LocalPath().config_directory)

    def test_all_results_directory_has_expected_value(self):
        self.assertEqual(self._current_path.parent.joinpath("s3-results"), LocalPath().all_results_directory)

    def test_analysis_date_time_file_has_expected_value(self):
        self.assertEqual(
            self._current_path.parent.joinpath("s3-results/analysis_date_time.txt"),
            LocalPath().analysis_date_time_file,
        )
