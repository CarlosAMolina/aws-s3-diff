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
from src.analysis import _AllAccoutsS3DataDfAnalyzer
from src.analysis import _AnalysisDfToCsv
from src.analysis import _CompareAwsAccounts
from src.analysis import _DfAnalysis
from src.analysis import _OriginFileSyncDfAnalysis
from src.analysis import _TargetAccountWithoutMoreFilesDfAnalysis
from src.s3_uris_to_analyze import S3UrisFileAnalyzer


class _DfAnalysisConfig(ABC):
    @property
    @abstractmethod
    def analysis_class_to_check(self) -> type[_DfAnalysis]:
        pass

    @property
    @abstractmethod
    def file_name_and_expected_result(self) -> dict[str, list]:
        pass

    @property
    @abstractmethod
    def column_name_to_check(self) -> str:
        pass


class _OriginFileSyncDfAnalysisConfig(_DfAnalysisConfig):
    @property
    def analysis_class_to_check(self) -> type[_DfAnalysis]:
        return _OriginFileSyncDfAnalysis

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


class _TargetAccountWithoutMoreFilesDfAnalysisConfig(_DfAnalysisConfig):
    @property
    def analysis_class_to_check(self) -> type[_DfAnalysis]:
        return _TargetAccountWithoutMoreFilesDfAnalysis

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
        cls._s3_uris_file_analyzer = S3UrisFileAnalyzer()

    # TODO rename folder `test-origin-file-sync`
    @patch(
        "src.analysis.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_for_several_df_analysis(self, mock_directory_path_what_to_analyze):
        for analysis_config in [_OriginFileSyncDfAnalysisConfig(), _TargetAccountWithoutMoreFilesDfAnalysisConfig()]:
            self._test_get_df_set_analysis_for_several_file_cases(analysis_config)

    def _test_get_df_set_analysis_for_several_file_cases(self, config: _DfAnalysisConfig):
        for file_name, expected_result in config.file_name_and_expected_result.items():
            df = self._get_df_combine_accounts_s3_data_csv(file_name)
            result = config.analysis_class_to_check(self._aws_accounts_to_compare, df).get_df_set_analysis()
            result_to_check = result.loc[:, ("analysis", config.column_name_to_check)].tolist()
            self.assertEqual(expected_result, result_to_check)

    def _get_df_combine_accounts_s3_data_csv(self, file_name: str) -> Df:
        file_path_name = f"fake-files/test-origin-file-sync/s3-files-all-accounts/{file_name}"
        return _get_df_combine_accounts_s3_data_csv(file_path_name)

    @property
    def _aws_accounts_to_compare(self) -> _CompareAwsAccounts:
        all_aws_accounts = self._s3_uris_file_analyzer.get_aws_accounts()
        return _CompareAwsAccounts(*all_aws_accounts[:2])


class TestAllAccoutsS3DataDfAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    @patch(
        "src.analysis.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_get_df_set_analysis(
        self,
        mock_directory_path_what_to_analyze,
    ):
        df = _get_df_combine_accounts_s3_data_csv("fake-files/s3-results/20241201180132/s3-files-all-accounts.csv")
        result = _AllAccoutsS3DataDfAnalyzer().get_df_set_analysis(df)
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
    return _CombinedAccountsS3DataCsvToDf().get_df(Path(__file__).parent.absolute().joinpath(file_path_name))
