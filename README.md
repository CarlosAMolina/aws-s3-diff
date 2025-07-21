# AWS S3 Diff

## Introduction

The purpose of this program is to compare files between AWS S3 URIs.

The URIs can be in different AWS accounts and buckets.

Analysis information can be added. For example, if the hash of the files match.

## Documentation

Please read the documentation at [this URL](https://cmoli.es/projects/aws-s3-diff/aws-s3-diff.html).

## Example results

Example of [final results file](https://github.com/CarlosAMolina/aws-s3-diff/blob/main/tests/expected-results/if-queries-with-results/analysis.csv).

Example of the CLI output if two accounts have been analyzed:

```bash
$ awsume pro

$ make run
poetry run python run.py
[INFO] Welcome to the AWS S3 Diff tool!
[DEBUG] Checking if the URIs to analyze configuration file is correct
[INFO] AWS accounts configured to be analyzed:
1. pro
2. dev
[DEBUG] Creating the directory: /home/user/Software/aws-s3-diff/s3-results/20250519224348
[INFO] Analyzing the AWS account 'pro'
[INFO] Analyzing S3 URI 1/2: s3://pets/dogs/
[INFO] Analyzing S3 URI 2/2: s3://pets/cats/
[INFO] Exporting /home/user/Software/aws-s3-diff/s3-results/20250519224348/pro.csv
[INFO] The next account to be analyzed is 'dev'. Authenticate and run the program again

$ awsume dev

$ make run
poetry run python run.py
[INFO] Welcome to the AWS S3 Diff tool!
[DEBUG] Checking if the URIs to analyze configuration file is correct
[INFO] AWS accounts configured to be analyzed:
1. pro
2. dev
[INFO] Analyzing the AWS account 'dev'
[INFO] Analyzing S3 URI 1/2: s3://pets-dev/doggies/
[INFO] Analyzing S3 URI 2/2: s3://pets-dev/kitties/
[INFO] Exporting /home/user/Software/aws-s3-diff/s3-results/20250519224348/dev.csv
[INFO] Exporting /home/user/Software/aws-s3-diff/s3-results/20250519224348/s3-files-all-accounts.csv
[INFO] Analyzing if files of the account 'pro' have the same hash as in account 'dev'
[INFO] Analyzing if files in account 'dev' can exist, compared to account 'pro'
[INFO] Exporting /home/user/Software/aws-s3-diff/s3-results/20250519224348/analysis.csv
[DEBUG] Removing: /home/user/Software/aws-s3-diff/s3-results/analysis_date_time.txt
```
