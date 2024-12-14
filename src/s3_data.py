import csv
from pathlib import Path

from local_results import LocalResults
from s3_client import S3Client
from s3_uris_to_analyze import S3UrisFileReader
from types_custom import S3Data
from types_custom import S3Query


def extract_s3_data_of_account(aws_account: str):
    _AwsAccountExtractor(
        LocalResults().get_file_path_aws_account_results(aws_account),
        S3UrisFileReader().get_s3_queries_for_aws_account(aws_account),
    ).extract()


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
