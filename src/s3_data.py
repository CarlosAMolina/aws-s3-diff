from local_results import LocalResults
from s3_extract import AwsAccountExtractor
from s3_uris_to_analyze import S3UrisFileReader


def extract_s3_data_of_account(aws_account: str):
    AwsAccountExtractor(
        LocalResults().get_file_path_aws_account_results(aws_account),
        S3UrisFileReader().get_s3_queries_for_aws_account(aws_account),
    ).extract()
