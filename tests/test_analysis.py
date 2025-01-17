import unittest
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from s3_data import _CombinedAccountsS3DataCsvToDf
from src.analysis import _AllAnalysisSetter
from src.analysis import _AnalysisDfToCsv
from src.analysis import _AnalysisSetter
from src.analysis import _CanFileExistAnalysisSetter
from src.analysis import _CompareAwsAccounts
from src.analysis import _IsFileCopiedAnalysisSetter
from src.config_files import S3UrisFileReader


class _AnalysisSetterConfig(ABC):
    @property
    @abstractmethod
    def analysis_class_to_check(self) -> type[_AnalysisSetter]:
        pass

    @property
    @abstractmethod
    def file_name_and_expected_result(self) -> dict[str, list]:
        pass

    @property
    @abstractmethod
    def column_name_to_check(self) -> str:
        pass


class _IsFileCopiedAnalysisSetterConfig(_AnalysisSetterConfig):
    @property
    def analysis_class_to_check(self) -> type[_AnalysisSetter]:
        return _IsFileCopiedAnalysisSetter

    @property
    def file_name_and_expected_result(self) -> dict[str, list]:
        return {
            "file-sync-ok.csv": [True],
            "file-sync-wrong.csv": [False],
            "file-not-in-origin.csv": [False],
            "file-not-in-target.csv": [False],
            "file-not-in-origin-target.csv": [True],
        }

    @property
    def column_name_to_check(self) -> str:
        return "is_sync_ok_in_aws_account_2_release"


class _CanFileExistAnalysisSetterConfig(_AnalysisSetterConfig):
    @property
    def analysis_class_to_check(self) -> type[_AnalysisSetter]:
        return _CanFileExistAnalysisSetter

    @property
    def file_name_and_expected_result(self) -> dict[str, list]:
        return {
            "file-sync-ok.csv": [None],
            "file-not-in-origin.csv": [False],
            "file-not-in-target.csv": [None],
            "file-not-in-origin-target.csv": [None],
        }

    @property
    def column_name_to_check(self) -> str:
        return "can_exist_in_aws_account_2_release"


class TestDfAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._s3_uris_file_reader = S3UrisFileReader()

    @patch(
        "src.analysis.S3UrisFileReader._file_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/s3-uris-to-analyze/to-test-possible-s3-files-all-accounts.csv"),
    )
    def test_get_df_set_analysis_result_for_several_df_analysis(self, mock_file_path_what_to_analyze):
        for analysis_config in [
            _IsFileCopiedAnalysisSetterConfig(),
            _CanFileExistAnalysisSetterConfig(),
        ]:
            self._run_test_get_df_set_analysis_for_several_file_cases(analysis_config)

    def _run_test_get_df_set_analysis_for_several_file_cases(self, config: _AnalysisSetterConfig):
        for file_name, expected_result in config.file_name_and_expected_result.items():
            df = self._get_df_combine_accounts_s3_data_csv(file_name)
            result = config.analysis_class_to_check(self._aws_accounts_to_compare, df).get_df_set_analysis()
            result_to_check = result.loc[:, ("analysis", config.column_name_to_check)].tolist()
            self.assertEqual(expected_result, result_to_check)

    def _get_df_combine_accounts_s3_data_csv(self, file_name: str) -> Df:
        file_path_name = f"fake-files/possible-s3-files-all-accounts/{file_name}"
        return _get_df_combine_accounts_s3_data_csv(file_path_name)

    @property
    def _aws_accounts_to_compare(self) -> _CompareAwsAccounts:
        all_aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        return _CompareAwsAccounts(*all_aws_accounts[:2])


class TestAllAnalysisSetter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    @patch(
        "src.analysis.S3UrisFileReader._file_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis/s3-uris-to-analyze.csv"),
    )
    def get_df_set_analysis_columns(self, mock_file_path_what_to_analyze):
        df = _get_df_combine_accounts_s3_data_csv("fake-files/test-full-analysis/s3-files-all-accounts.csv")
        result = _AllAnalysisSetter().get_df_set_analysis_columns(df)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = _AnalysisDfToCsv()._get_df_to_export(result).reset_index()
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        expected_result = expected_result.astype(
            {"is_sync_ok_in_aws_account_2_release": "object", "is_sync_ok_in_aws_account_3_dev": "object"}
        )
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


def _get_df_combine_accounts_s3_data_csv(file_path_name: str) -> Df:
    path = Path(__file__).parent.absolute().joinpath(file_path_name)
    return _CombinedAccountsS3DataCsvToDf().get_df(path)
