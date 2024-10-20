import unittest
from pathlib import Path

from pandas import DataFrame as Df
from pandas.testing import assert_frame_equal

from src import compare as m_compare

current_path = Path(__file__).parent.resolve()


class Test_get_df_combine_files(unittest.TestCase):
    def test_get_df_combine_files(self):
        result = m_compare._get_df_combine_files()
        expected_result = Df()
        assert_frame_equal(expected_result, result)
