from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df
from pandas import MultiIndex

from config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data.s3_client import S3Client
from types_custom import FileS3Data
from types_custom import MultiIndexDf
from types_custom import S3Data
from types_custom import S3Query


class AccountS3DataFactory:
    def __init__(self, account: str):
        self._account = account  # TODO deprecate
        self._account_extractor = _AccountExtractor(account)
        self._csv_reader = _CsvReader(account)
        self._multi_index_df_factory = _MultiIndexDfFactory(account)
        self._s3_uris_file_reader = S3UrisFileReader()

    def to_csv_extract_s3_data(self):
        self._account_extractor.extract_s3_data_to_csv()

    def get_df_from_csv(self) -> MultiIndexDf:
        df = self._csv_reader.get_df()
        return self._multi_index_df_factory.get_df(df)

    def get_df_from_csv_with_original_account_index(self) -> MultiIndexDf:
        result = self.get_df_from_csv()
        return _S3UriDfModifier(self._account).get_df_set_s3_uris_in_origin_account(result)


class _AccountExtractor:
    def __init__(self, account: str):
        self._account = account
        self._s3_uris_file_reader = S3UrisFileReader()
        self._local_results = LocalResults()
        self._logger = get_logger()

    def extract_s3_data_to_csv(self):
        self._logger.info(f"Exporting AWS account information to {self._file_path_results}")
        for query_index, s3_query in enumerate(self._get_s3_queries(), 1):
            self._logger.info(f"Analyzing S3 URI {query_index}/{len(self._get_s3_queries())}: {s3_query}")
            try:
                self._extract_s3_data_of_query(s3_query)
            except Exception as exception:
                self._drop_file()
                raise exception

    @property
    def _file_path_results(self) -> Path:
        return self._local_results.get_file_path_account_results(self._account)

    def _get_s3_queries(self) -> list[S3Query]:
        return self._s3_uris_file_reader.get_s3_queries_for_account(self._account)

    def _extract_s3_data_of_query(self, s3_query: S3Query):
        is_any_result = False
        for s3_data in S3Client(s3_query).get_s3_data():
            is_any_result = True
            self._export_s3_data_to_csv(s3_data, s3_query)
        if not is_any_result:
            self._export_s3_data_to_csv([FileS3Data()], s3_query)

    def _export_s3_data_to_csv(self, s3_data: S3Data, s3_query: S3Query):
        query_and_data_df = self._get_df_query_and_data(s3_data, s3_query)
        export_headers = not self._file_path_results.is_file()
        query_and_data_df.to_csv(header=export_headers, index=False, mode="a", path_or_buf=self._file_path_results)

    def _get_df_query_and_data(self, s3_data: S3Data, s3_query: S3Query) -> Df:
        result = Df(file_data._asdict() for file_data in s3_data)
        result.insert(0, "bucket", s3_query.bucket)
        result.insert(1, "prefix", s3_query.prefix)
        return result

    def _drop_file(self):
        self._file_path_results.unlink()


class _CsvReader:
    def __init__(self, account: str):
        self._account = account
        self._local_results = LocalResults()

    def get_df(self) -> Df:
        local_file_path_name = self._local_results.get_file_path_account_results(self._account)
        return pd.read_csv(
            local_file_path_name,
            index_col=["bucket", "prefix", "name"],
            parse_dates=["date"],
        ).astype({"size": "Int64"})


class _MultiIndexDfFactory:
    def __init__(self, account: str):
        self._account = account

    def get_df(self, df: Df) -> Df:
        result = df.copy()
        columns = self._get_index_as_mult_index(result.columns)
        result.columns = MultiIndex.from_tuples(columns)
        return result

    def _get_index_as_mult_index(self, index: pd.Index) -> list[tuple[str, str]]:
        return [(self._account, column_name) for column_name in index]


class _S3UriDfModifier:
    def __init__(self, account_target):
        self._account_target = account_target
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self, df: Df) -> Df:
        result = df.copy()
        s3_uris_map_df = self._s3_uris_file_reader.get_df_s3_uris_map_between_accounts(
            self._account_origin, self._account_target
        )
        if s3_uris_map_df[self._account_origin].equals(s3_uris_map_df[self._account_target]):
            return result
        return self._get_df_replace_index_with_s3_uris_map(result, s3_uris_map_df)

    @property
    def _account_origin(self) -> str:
        return self._s3_uris_file_reader.get_first_account()

    def _get_df_replace_index_with_s3_uris_map(self, df: Df, s3_uris_map_df: Df) -> Df:
        original_length = len(df)
        assert df.index.get_level_values("prefix").str.endswith("/").all()
        result = df.copy()
        result = result.reset_index("name")
        s3_uris_map_df = self._get_s3_uris_map_prepared_for_join(s3_uris_map_df)
        result = result.join(s3_uris_map_df)
        if True in result[[f"{self._account_origin}_bucket", f"{self._account_origin}_prefix"]].isna().any().values:
            raise ValueError("Some values could not be replaced")
        result = result.rename(
            columns={f"{self._account_origin}_bucket": "bucket", f"{self._account_origin}_prefix": "prefix"}
        )
        result = result.reset_index(drop=True).set_index(["bucket", "prefix", "name"])
        assert original_length == len(result)
        return result

    def _get_s3_uris_map_prepared_for_join(self, s3_uris_map_df: Df) -> Df:
        result = s3_uris_map_df.copy()
        for account in (self._account_origin, self._account_target):
            result[[f"{account}_bucket", f"{account}_prefix"]] = result[account].str.extract(
                REGEX_BUCKET_PREFIX_FROM_S3_URI, expand=False
            )
            result.loc[~result[f"{account}_prefix"].str.endswith("/"), f"{account}_prefix"] = (
                result[f"{account}_prefix"] + "/"
            )
        result.drop(columns=[f"{account}" for account in (self._account_origin, self._account_target)], inplace=True)
        result = result.rename(
            columns={f"{self._account_target}_bucket": "bucket", f"{self._account_target}_prefix": "prefix"}
        )
        result = result.set_index(["bucket", "prefix"])
        result.columns = MultiIndex.from_tuples(
            [(column, "") for column in result.columns]
        )  # To merge with a MultiIndex columns Df.
        return result
