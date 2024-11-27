import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from src import compare as m_compare
from src.config import _S3UrisFile
from src.config import Config
from src.constants import FOLDER_NAME_S3_RESULTS


class TestS3DataComparator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    @patch("src.analysis.LocalResults._get_analysis_date_time_str")
    @patch("src.analysis.LocalResults.path_directory_all_results")
    @patch("src.config._S3UrisFile.file_path")
    def test_get_df_s3_data_analyzed(
        self, mock_file_what_to_analyze_path, mock_path_directory_all_results, mock_get_analysis_date_time_str
    ):
        current_path = Path(__file__).parent.absolute()
        mock_file_what_to_analyze_path.return_value = current_path.joinpath(
            "fake-files", _S3UrisFile._FILE_NAME_S3_URIS
        )
        mock_path_directory_all_results.return_value = current_path.joinpath("fake-files", FOLDER_NAME_S3_RESULTS)
        mock_get_analysis_date_time_str.return_value = "exports-all-aws-accounts"  # TODO use datetime str
        config = Config()
        result = m_compare.S3DataComparator()._get_df_s3_data_analyzed(config)
        # m_compare.S3DataComparator().run(config)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = (
            m_compare._CsvExporter()
            ._get_df_to_export(result)
            .reset_index()
            .astype({"is_sync_ok_in_aws_account_2_release": "str", "is_sync_ok_in_aws_account_3_dev": "str"})
        )
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        result_as_csv_export = result_as_csv_export.replace({np.nan: None})
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected-results", "analysis.csv")
        result = read_csv(expected_result_file_path).astype(
            {
                "aws_account_1_pro_size": "Int64",
                "aws_account_2_release_size": "Int64",
                "aws_account_3_dev_size": "Int64",
            }
        )
        # https://stackoverflow.com/a/26763793
        date_column_names = ["aws_account_1_pro_date", "aws_account_2_release_date", "aws_account_3_dev_date"]
        result[date_column_names] = result[date_column_names].apply(to_datetime)
        return result
