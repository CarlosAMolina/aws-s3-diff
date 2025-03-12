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
	poetry run python src/main.py

run-using-local-s3-server:
	export AWS_ENDPOINT=http://localhost:5000 && poetry run python src/main.py

start-local-s3-server:
	poetry run python tests/run_local_s3_server.py

test:
	poetry run python -m unittest discover

test-filter:
	#poetry run python -m unittest discover -p test_config_files.py -k TestAnalysisConfigChecker
	#poetry run python -m unittest discover -p test_s3_data.py -k TestS3UriDfModifier
	poetry run python -m unittest discover -p test_s3_data_analysis.py -k TestDfAnalysis.test_get_df_set_analysis_result_for_several_df_analysis
	#poetry run python -m unittest discover -p test_with_local_s3_server.py -k TestWithLocalS3Server.test_run_test_s3_data
	#poetry run python -m unittest discover -p test_main.py -k TestMainWithoutLocalS3Server.test_run_manages_analysis_config_error_and_generates_expected_error_messages
	#poetry run python -m unittest discover -p test_main.py -k TestMainWithLocalS3Server.test_run_if_should_work_ok
	#poetry run python -m unittest discover -p test_s3_client.py

