import unittest
from pathlib import Path

from pandas import DataFrame as Df
from pandas.testing import assert_frame_equal

from src import compare as m_compare
from src.config import Config

current_path = Path(__file__).parent.resolve()


class Test_get_df_combine_files(unittest.TestCase):
    def test_get_df_combine_files(self):
        current_path = Path(__file__).parent.absolute()
        path_src = current_path.parent.joinpath("src")
        path_config_files = path_src
        path_with_folder_exported_s3_data = current_path
        config = Config(path_config_files, path_with_folder_exported_s3_data)
        result = m_compare._get_df_combine_files(config)
        expected_result = Df()
        assert_frame_equal(expected_result, result)