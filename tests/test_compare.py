import unittest
from pathlib import Path

from pandas import DataFrame as Df
from pandas import read_csv
from pandas.testing import assert_frame_equal

from src import compare as m_compare
from src.config import Config


class Test_get_df_combine_files(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    def test_get_df_combine_files(self):
        config = self._get_config_for_the_test()
        result = m_compare._get_df_combine_files(config)
        result_as_csv_export = m_compare._CsvExporter()._get_df_to_export(result).reset_index()
        expected_result = self._get_df_from_csv_expected_result()
        print(">>> EXPECTED RESULT")
        print(expected_result)
        print(">>> RESULT_AS_CSV_EXPORT")
        print(result_as_csv_export)
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_config_for_the_test(self) -> Config:
        path_src = self.current_path.parent.joinpath("src")
        path_config_files = path_src
        path_with_folder_exported_s3_data = self.current_path
        return Config(path_config_files, path_with_folder_exported_s3_data)

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected_result_compare.csv")
        return read_csv(expected_result_file_path).astype(
            {
                "aws_account_1_pro_size": "Int64",
                "aws_account_2_release_size": "Int64",
                "aws_account_3_dev_size": "Int64",
            }
        )
