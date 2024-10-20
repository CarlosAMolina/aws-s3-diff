ruff-check:
	poetry run ruff check

ruff-check-fix:
	poetry run ruff check --fix

ruff-format:
	poetry run ruff format

test:
	poetry run python -m unittest discover
