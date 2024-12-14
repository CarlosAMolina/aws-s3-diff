# AWS S3 Diff

## Introduction

This project allows you to compare AWS S3 files between different AWS accounts.

## How to run the program

To install the project dependencies, run `poetry install`.

After that, we need to configure the `s3-uris-to-analyze.csv` file. Once it is done, execute `make run` and follow the instructions.

### s3-uris-to-analyze.csv file configuration

The file to configure [is here](config/s3-uris-to-analyze.csv).

File structure:

- It is a `.csv` file separated by `,`.
- Each column represents an AWS account configuration.
- The first row is special, it is where to specify the account names.
- The other rows are the S3 URIs to be analyzed.

The order in which the AWS accounts are specified is the order in which they will be analyzed.

### Run the program

In this step we have already configured the `s3-uris-to-analyze.csv` file.

Authenticate in the terminal to the first AWS account that will be analyzed, this is required in order to allow botocore to connect with your AWS account. Execute:

```bash
make run
```

Now, we have the results for the buckets of the first account. Let's create the second AWS account results!

We authenticate in the terminal to the second AWS account and run `make run` again. The script will detect that the first account results exist and will analyze the second account.

We repeat the previous steps per each configured AWS account:

1. Authenticate in the terminal to the AWS account to analyze.
2. Execute `make run`

The analysis results are stored in the [s3-results](s3-results) folder, a folder with the current analysis timestamp is created and all the accounts results are stored in that folder. The final file with all the results and the analysis is called `analysis.csv`, you can open and examine that file.

## Develop

```bash
poetry install --all-extras
poetry run pre-commit install
```

## Testing

### Run all tests

```bash
make test
```

### Access Moto Server using AWS CLI

```bash
make moto-run-local-server
make awscli-local-s3-ls
```
