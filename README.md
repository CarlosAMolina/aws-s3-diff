# AWS S3 Diff

## Introduction

This project allows you to compare AWS S3 files between different AWS accounts.

## How to run the program

To install the project dependencies, run `poetry install`.

After that, we need to configure the `s3-uris-to-analyze.csv` file. Once it is done, execute `make run` and follow the instructions.

### s3-uris-to-analyze.csv file configuration

The file to configure with your AWS information to analyze [is here](config/s3-uris-to-analyze.csv).

File structure:

- It is a `.csv` file separated by `,`.
- Each column represents an AWS account configuration.
- The first row is special, is where the account names are specified.
- The other rows are the S3 URIs to be analyzed.

The order in which the AWS accounts are specified is the order in which they will be analyzed.

### analysis_config.json file configuration

This file specifies what to analyzed abut the extracted AWS information.

File path: [here](config/s3-uris-to-analyze.csv).

You can configure the values of the following keys (do not modify the keys, only the values):

Key                   | Type of the value | What is it?
----------------------|-------------------|---------------------------------------------------------------------------
origin                | String            | The reference account to compare other accounts.
is_the_file_copied_to | Array of strings  | Checks if the file in the origin account has been copied to other accounts.
can_the_file_exist_in | Array of strings  | If the file does not exist in the origin account, it cannot exist in other accounts.

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

### Run local S3 server to make requests

Start the local server:

```bash
make start-local-s3-server
```

After that, you can:

    - List files:

    ```bash
    make awscli-local-s3-ls
    ```

    - Run the CLI:

    ```bash
    make run-using-local-s3-server
    ```
