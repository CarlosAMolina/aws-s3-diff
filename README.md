# AWS S3 Diff

Warning. This project is working correctly but is under development. It will change until the version 1.0.0 is released.

## Introduction

This project allows you to compare AWS S3 files between different AWS accounts.

Example of results file: [here](tests/expected-results/analysis.csv).

## How to run the program

To install the project dependencies, run `poetry install`.

After that, we need to configure the `s3-uris-to-analyze.csv` file. Once it is done, execute `make run` and follow the instructions.

### s3-uris-to-analyze.csv file configuration

The file to configure with your AWS information to analyze [is here](config/s3-uris-to-analyze.csv).

File structure:

- It is a `.csv` file separated by `,`.
- Each column represents an AWS account configuration.
- The first row is special, is where the account names are specified. This information will be used in the CLI outputs and in the result files to organize the data.
- The other rows are the S3 URIs to be analyzed.

The order in which the AWS accounts are specified is the order in which they will be analyzed.

### analysis_config.json file configuration

This file specifies what to analyzed abut the extracted AWS information.

File path: [here](config/analysis-config.json).

You can configure the values of the following keys (do not modify the keys, only the values):

Key                   | Type of the value | What is it?
----------------------|-------------------|---------------------------------------------------------------------------
run_analysis          | Boolean           | If the analysis should be executed.
origin                | String            | The reference account to compare other accounts.
is_the_file_copied_to | Array of strings  | Checks if the file in the origin account has been copied to other accounts.
can_the_file_exist_in | Array of strings  | If the file does not exist in the origin account, it cannot exist in other accounts.

### Run the program

In this step we have already update the previous configuration files with the desired values.

Authenticate in the terminal to the first AWS account that will be analyzed, this is required in order to allow botocore to connect with your AWS account. For examle:

```bash
awsume dev
```

After that, execute:

```bash
make run
```

Now, we have the results for the buckets of the first account ([file example](tests/expected-results/dev.csv)). Let's create the second AWS account results!

We authenticate in the terminal to the second AWS account and run `make run` again. The script will detect that the first account results exist and will analyze the second account.

We repeat the previous steps per each configured AWS account:

1. Authenticate in the terminal to the AWS account to analyze.
2. Execute `make run`

The analysis results are stored in the [s3-results](s3-results) folder, a folder with the current analysis timestamp is created and all the accounts results are stored in that folder. The final file with all the results and the analysis is called `analysis.csv`, you can open and examine that file ([example](tests/expected-results/analysis.csv)).

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
[INFO] Analyzing S3 URI 2/2: s3://pets/inbound/cats/
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
[INFO] Analyzing if files of the account 'pro' have been copied to the account 'dev'
[INFO] Analyzing if files in account 'dev' can exist, compared to account 'pro'
[INFO] Exporting /home/user/Software/aws-s3-diff/s3-results/20250519224348/analysis.csv
[DEBUG] Removing: /home/user/Software/aws-s3-diff/s3-results/analysis_date_time.txt
```

## FAQ

- Can I analyze different paths for the same account?

  Yes, instead of managing each s3-uris-to-analyze.csv column as a different account, configure them using the desired paths of the target AWS account. Despite being the same AWS account, a different name must be used in the first row for each column.

- Can I analyze a path that contains subfolders?

  No, this is a current limitation. The analyzed paths must contain files only, not folders.

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
