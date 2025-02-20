from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df
from pandas import MultiIndex

from config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data.interface import AsMultiIndexFactory
from s3_data.interface import CsvFactory
from s3_data.interface import CsvReader
from s3_data.interface import FromCsvDfFactory
from s3_data.interface import IndexFactory
from s3_data.interface import NewDfFactory
from s3_data.s3_client import S3Client
from types_custom import FileS3Data
from types_custom import MultiIndexDf
from types_custom import S3Data
from types_custom import S3Query


class AccountCsvFactory(CsvFactory):
    def __init__(self, account: str):
        self._account = account
        self._account_new_df_factory = _AccountNewDfFactory(account)
        self._local_results = LocalResults()
        self._logger = get_logger()

    def to_csv(self):
        self._logger.info(f"Exporting AWS account information to {self._file_path_results}")
        try:
            result_df = self._account_new_df_factory.get_df()
            result_df.to_csv(index=False, path_or_buf=self._file_path_results)
        except Exception as exception:
            self._drop_file()
            raise exception

    @property
    def _file_path_results(self) -> Path:
        return self._local_results.get_file_path_account_results(self._account)

    def _drop_file(self):
        self._file_path_results.unlink()


class AccountFromCsvFactory(FromCsvDfFactory):
    def __init__(self, account: str):
        self._csv_reader = _AccountCsvReader(account)
        self._account_as_multi_index_factory = _AccountAsMultiIndexFactory(account)
        self._account_with_origin_s3_uris_index_factory = _AccountWithOriginS3UrisIndexFactory(account)

    def get_df(self) -> MultiIndexDf:
        df = self._csv_reader.get_df()
        return self._account_as_multi_index_factory.get_df(df)

    def get_df_with_original_account_index(self) -> MultiIndexDf:
        result = self.get_df()
        return self._account_with_origin_s3_uris_index_factory.get_df(result)


class _AccountNewDfFactory(NewDfFactory):
    def __init__(self, account: str):
        self._account = account
        self._s3_uris_file_reader = S3UrisFileReader()
        self._logger = get_logger()

    def get_df(self) -> Df:
        result = Df()
        for query_index, s3_query in enumerate(self._get_s3_queries(), 1):
            self._logger.info(f"Analyzing S3 URI {query_index}/{len(self._get_s3_queries())}: {s3_query}")
            for s3_data in self._get_s3_data_of_query(s3_query):
                query_and_data_df = self._get_df_from_s3_data_and_query(s3_data, s3_query)
                result = pd.concat([result, query_and_data_df])
        return result

    def _get_s3_queries(self) -> list[S3Query]:
        return self._s3_uris_file_reader.get_s3_queries_for_account(self._account)

    def _get_s3_data_of_query(self, s3_query: S3Query) -> Iterator[S3Data]:
        is_any_result = False
        for s3_data in S3Client(s3_query).get_s3_data():
            is_any_result = True
            yield s3_data
        # TODO? try deprecate, maybe it is required to avoid exception when no results
        if not is_any_result:
            yield [FileS3Data()]

    def _get_df_from_s3_data_and_query(self, s3_data: S3Data, s3_query: S3Query) -> Df:
        result = Df(file_data._asdict() for file_data in s3_data)
        result.insert(0, "bucket", s3_query.bucket)
        result.insert(1, "prefix", s3_query.prefix)
        return result


class _AccountCsvReader(CsvReader):
    def __init__(self, account: str):
        self._account = account
        self._local_results = LocalResults()

    def get_df(self) -> Df:
        local_file_path_name = self._local_results.get_file_path_account_results(self._account)
        return pd.read_csv(
            local_file_path_name,
            parse_dates=["date"],
        ).astype({"size": "Int64"})


class _AccountAsMultiIndexFactory(AsMultiIndexFactory):
    def __init__(self, account: str):
        self._account = account

    def get_df(self, df: Df) -> MultiIndexDf:
        result = df.copy()
        result = self._get_df_with_multi_index(result)
        return self._get_df_with_multi_index_columns(result)

    def _get_df_with_multi_index(self, df: Df) -> MultiIndex:
        return df.set_index(["bucket", "prefix", "name"], drop=True)

    def _get_df_with_multi_index_columns(self, df: Df | MultiIndexDf) -> MultiIndexDf:
        result = df
        columns = self._get_index_as_mult_index(result.columns)
        result.columns = MultiIndex.from_tuples(columns)
        return result

    def _get_index_as_mult_index(self, index: pd.Index) -> list[tuple[str, str]]:
        return [(self._account, column_name) for column_name in index]


class _AccountWithOriginS3UrisIndexFactory(IndexFactory):
    def __init__(self, account_target):
        self._account_target = account_target
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self, df: Df) -> MultiIndexDf:
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
