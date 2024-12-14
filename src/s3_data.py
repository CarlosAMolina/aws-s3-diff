import csv
from pathlib import Path

from pandas import DataFrame as Df

from combine import get_df_combine_files
from local_results import LocalResults
from s3_client import S3Client
from s3_uris_to_analyze import S3UrisFileReader
from types_custom import S3Data
from types_custom import S3Query


def export_s3_data_of_account(aws_account: str):
    _AwsAccountExtractor(
        LocalResults().get_file_path_aws_account_results(aws_account),
        S3UrisFileReader().get_s3_queries_for_aws_account(aws_account),
    ).extract()


def export_s3_data_of_all_accounts():
    s3_data_df = get_df_combine_files()  # TODO move to combine.py
    _CombineDfToCsv().export(s3_data_df)  # TODO move to combine.py


class _AwsAccountExtractor:
    def __init__(self, file_path_results: Path, s3_queries: list[S3Query]):
        self._file_path_results = file_path_results
        self._s3_queries = s3_queries

    def extract(self):
        print(f"Extracting AWS Account information to {self._file_path_results}")
        for query_index, s3_query in enumerate(self._s3_queries, 1):
            print(f"Running query {query_index}/{len(self._s3_queries)}: {s3_query}")
            s3_data = S3Client().get_s3_data(s3_query)
            self._export_data_to_csv(s3_data, s3_query)
        print("Extraction done")

    def _export_data_to_csv(self, s3_data: S3Data, s3_query: S3Query):
        file_exists = self._file_path_results.exists()
        with open(self._file_path_results, "a", newline="") as f:
            # avoid ^M: https://stackoverflow.com/a/17725590
            headers = {**s3_query._asdict(), **s3_data[0]}.keys()
            w = csv.DictWriter(f, headers, lineterminator="\n")
            if not file_exists:
                w.writeheader()
            for file_data in s3_data:
                data = {**s3_query._asdict(), **file_data}
                w.writerow(data)


# TODO move to combine.py
class _CombineDfToCsv:
    def export(self, df: Df):
        file_path = LocalResults().get_file_path_s3_data_all_accounts()
        print(f"Exporting all AWS accounts S3 files information to {file_path}")
        csv_df = self._get_df_to_export(df)
        csv_df.to_csv(file_path)

    def _get_df_to_export(self, df: Df) -> Df:
        result = df.copy()
        csv_column_names = ["_".join(values) for values in result.columns]
        csv_column_names = [
            self._get_csv_column_name_drop_undesired_text(column_name) for column_name in csv_column_names
        ]
        result.columns = csv_column_names
        aws_account_1 = S3UrisFileReader().get_aws_accounts()[0]
        result.index.names = [
            f"bucket_{aws_account_1}",
            f"file_path_in_s3_{aws_account_1}",
            "file_name_all_aws_accounts",
        ]
        return result

    def _get_csv_column_name_drop_undesired_text(self, column_name: str) -> str:
        if column_name.startswith("analysis_"):
            return column_name.replace("analysis_", "", 1)
        return column_name