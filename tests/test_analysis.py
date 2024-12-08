import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from local_results import LocalResults
from src.analysis import _AnalysisDfToCsv
from src.analysis import S3DataAnalyzer


class TestS3DataAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    @patch("src.combine.LocalResults._get_analysis_date_time_str")
    @patch("src.combine.LocalResults._get_path_directory_all_results")
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_get_df_s3_data_analyzed(
        self, mock_directory_path_what_to_analyze, mock_get_path_directory_all_results, mock_get_analysis_date_time_str
    ):
        mock_get_path_directory_all_results.return_value = self.current_path.joinpath(
            "fake-files", LocalResults._FOLDER_NAME_S3_RESULTS
        )
        mock_get_analysis_date_time_str.return_value = "20241201180132"
        result = S3DataAnalyzer()._get_df_s3_data_analyzed()
        # S3DataAnalyzer().run(config)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = (
            _AnalysisDfToCsv()
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
        # https://stackoverflow.com/questions/26763344/convert-pandas-column-to-datetime/26763793#26763793
        date_column_names = ["aws_account_1_pro_date", "aws_account_2_release_date", "aws_account_3_dev_date"]
        result[date_column_names] = result[date_column_names].apply(to_datetime)
        return result
