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
	poetry run python -m unittest discover -p test_main.py -k TestFunction_run
