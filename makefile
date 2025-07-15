# https://github.com/getmoto/moto/issues/3390
awscli-local-s3-ls:
	aws --endpoint-url http://localhost:5000 s3 ls

awscli-local-s3-ls-bucket:
	aws --endpoint-url http://localhost:5000 s3 ls s3://pets --recursive

ruff-check:
	poetry run ruff check

ruff-check-fix:
	poetry run ruff check --fix

ruff-format:
	poetry run ruff format

run:
	poetry run python run.py

run-using-local-s3-server:
	export AWS_ENDPOINT=http://localhost:5000 && poetry run python run.py

start-local-s3-server:
	poetry run python tests/run_local_s3_server.py

test:
	poetry run python -m unittest discover

test-filter:
	#poetry run python -m unittest discover -p test_aws_s3_diff.py -k TestMainWithLocalS3Server.test_run_all_acounts_generates_expected_results_if_queries_with_results
	#poetry run python -m unittest discover -p test_aws_s3_diff.py -k TestMainWithoutLocalS3Server.test_run_manages_aws_client_errors_and_generates_expected_error_messages
	#poetry run python -m unittest discover -p test_config_files.py -k TestAnalysisConfigChecker
	#poetry run python -m unittest discover -p test_s3_client.py
	#poetry run python -m unittest discover -p test_s3_data.py -k TestS3UriDfModifier
	poetry run python -m unittest discover -p test_s3_data_analysis.py -k TestAnalysisDataGenerator
	#poetry run python -m unittest discover -p test_s3_data_one_account.py -k TestAccountDataGenerator.test_get_df_returns_expected_result_if_query_without_results

