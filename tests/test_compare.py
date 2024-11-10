import datetime
import unittest
from pathlib import Path

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from src import compare as m_compare
from tests.config import get_config_for_the_test


class TestS3DataComparator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    def test_get_df_s3_data_analyzed(self):
        config = get_config_for_the_test()
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
        expected_result = expected_result.replace({np.nan: None})
        result_as_csv_export = result_as_csv_export.replace({np.nan: None})
        assert_frame_equal(expected_result, result_as_csv_export)

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


class TestFunction_get_file_df_set_index(unittest.TestCase):
    def test_if_no_empty_file_df(self):
        date_time = datetime.datetime(2024, 10, 14, 8, 49, 1)
        df = Df(
            {
                "aws_account_1_pro_value_date": {"dogs_20241014.csv": date_time},
                "aws_account_1_pro_value_size": {"dogs_20241014.csv": 33201},
            }
        )
        result = m_compare._get_file_df_update_index("pets", df, "dogs_big_size.csv")
        index = "pets_path_dogs_big_size_file_dogs_20241014.csv"
        expected_result = Df(
            {"aws_account_1_pro_value_date": {index: date_time}, "aws_account_1_pro_value_size": {index: 33201}}
        )
        assert_frame_equal(expected_result, result)

    def test_if_empty_file_df(self):
        df = Df(
            {
                "aws_account_1_pro_value_date": {},
                "aws_account_1_pro_value_size": {},
            }
        )
        expected_result = df.copy()
        result = m_compare._get_file_df_update_index("pets", df, "dogs_big_size.csv")
        assert_frame_equal(expected_result, result)
