# https://github.com/getmoto/moto/issues/3390
awscli-local-s3-ls:
	aws --endpoint-url http://localhost:5000 s3 ls

awscli-local-s3-ls-bucket:
	aws --endpoint-url http://localhost:5000 s3 ls s3://pets --recursive

moto-run-local-server:
	poetry run python tests/run_moto_server.py

ruff-check:
	poetry run ruff check

ruff-check-fix:
	poetry run ruff check --fix

ruff-format:
	poetry run ruff format

run:
	poetry run python src/main.py

test:
	poetry run python -m unittest discover

test-filter:
	#poetry run python -m unittest discover -p test_s3_uris_to_analyze.py -k TestS3UrisFileChecker
	#poetry run python -m unittest discover -p test_combine.py -k TestS3UriDfModifier
	poetry run python -m unittest discover -p test_analysis.py -k TestOriginFileSyncDfAnalysis.test_get_df_set_analysis_result_if_file_sync_is_ok
	#poetry run python -m unittest discover -p test_main.py -k TestFunction_run
