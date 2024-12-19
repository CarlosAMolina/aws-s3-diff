import unittest
from pathlib import Path
from unittest import mock

from pandas import DataFrame as Df
from pandas import MultiIndex
from pandas import read_csv as read_csv_as_df
from pandas.testing import assert_frame_equal

from src import s3_data as m_s3_data
from src.local_results import _MainPaths
from src.local_results import LocalResults
from src.s3_uris_to_analyze import S3UrisFileReader
from tests.aws import S3Server

ExpectedResult = list[dict]


class TestAwsAccountExtractor(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        self._s3_server = S3Server()
        # Drop file created by the user or by other tests.
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_extract_generates_expected_result(self, mock_directory_path_what_to_analyze):
        LocalResults().create_analysis_results_folder()
        self._s3_server.start()
        for aws_account, file_path_name_expected_result in {
            "aws_account_1_pro": "tests/fake-files/s3-results/20241201180132/aws_account_1_pro.csv",
            "aws_account_2_release": "tests/fake-files/s3-results/20241201180132/aws_account_2_release.csv",
            "aws_account_3_dev": "tests/fake-files/s3-results/20241201180132/aws_account_3_dev.csv",
        }.items():
            self._s3_server.create_objects(aws_account)
            file_path_results = LocalResults().get_file_path_aws_account_results(aws_account)
            s3_queries = S3UrisFileReader().get_s3_queries_for_aws_account(aws_account)
            m_s3_data._AwsAccountExtractor(file_path_results, s3_queries).extract()
            result_df = read_csv_as_df(file_path_results)
            expected_result_df = read_csv_as_df(file_path_name_expected_result)
            expected_result_df["date"] = result_df["date"]
            assert_frame_equal(expected_result_df, result_df)
        self._s3_server.stop()


class TestS3UriDfModifier(unittest.TestCase):
    def test_get_df_modify_buckets_and_paths(self):
        old_multi_index = MultiIndex.from_tuples(
            [
                ("cars_dev", "europe/spain", "cars-20241014.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20240914.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241015.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241019.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241021.csv"),
                ("pets_dev", "horses/europe", "horses-20210219.csv"),
                ("pets_dev", "non-existent-prefix", None),
            ],
            names=["bucket", "prefix", "name"],
        )
        df = Df(index=old_multi_index)
        s3_uris_map_df = Df(
            {
                "aws_account_1_pro": {
                    0: "s3://cars/europe/spain",
                    1: "s3://pets/dogs/big_size",
                    2: "s3://pets/horses/europe",
                    3: "s3://pets/non-existent-prefix",
                },
                "aws_account_3_dev": {
                    0: "s3://cars_dev/europe/spain",
                    1: "s3://pets_dev/dogs/size/heavy",
                    2: "s3://pets_dev/horses/europe",
                    3: "s3://pets_dev/non-existent-prefix",
                },
            }
        )
        result = m_s3_data._S3UriDfModifier(
            "aws_account_1_pro", "aws_account_3_dev", df
        )._get_df_modify_buckets_and_paths(s3_uris_map_df)
        expected_result = Df(
            index=MultiIndex.from_tuples(
                [
                    ("cars", "europe/spain", "cars-20241014.csv"),
                    ("pets", "dogs/big_size", "dogs-20240914.csv"),
                    ("pets", "dogs/big_size", "dogs-20241015.csv"),
                    ("pets", "dogs/big_size", "dogs-20241019.csv"),
                    ("pets", "dogs/big_size", "dogs-20241021.csv"),
                    ("pets", "horses/europe", "horses-20210219.csv"),
                    ("pets", "non-existent-prefix", None),
                ],
                names=["bucket", "prefix", "name"],
            )
        )
        assert_frame_equal(expected_result, result)
