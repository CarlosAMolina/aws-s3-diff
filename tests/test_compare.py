import unittest
from pathlib import Path

from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from src import compare as m_compare
from src.config import Config


class TestS3DataComparator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    def test_get_df_s3_data_analyzed(self):
        config = self._get_config_for_the_test()
        result = m_compare._S3DataComparator()._get_df_s3_data_analyzed(config)
        # m_compare._S3DataComparator().run(config)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = (
            m_compare._CsvExporter()
            ._get_df_to_export(result)
            .reset_index()
            .astype({"is_sync_ok_in_aws_account_2_release": "str", "is_sync_ok_in_aws_account_3_dev": "str"})
        )
        expected_result = self._get_df_from_csv_expected_result()
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_config_for_the_test(self) -> Config:
        path_src = self.current_path.parent.joinpath("src")
        path_config_files = path_src
        return Config(path_config_files)

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected_result_compare.csv")
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
