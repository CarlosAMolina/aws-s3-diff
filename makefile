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
	#poetry run python -m unittest discover -p test_s3_uris_to_analyze.py -k TestS3UrisFileChecker
	#poetry run python -m unittest discover -p test_combine.py -k TestS3UriDfModifier
	#poetry run python -m unittest discover -p test_analysis.py #-k TestOriginFileSyncDfAnalysis.test_get_df_set_analysis_result_if_file_sync_is_ok
	#poetry run python -m unittest discover -p test_with_local_s3_server.py -k TestWithLocalS3Server.test_run_test_s3_data
	poetry run python -m unittest discover -p test_main.py -k TestFunction_runNoLocalS3Server.test_run_manages_no_aws_credentials #testFunction_runLocalS3Server.test_run_if_should_work_ok
