RUN = poetry run

.PHONY: install
install:
	poetry install

.PHONY: test
test: install
	$(RUN) python -m pytest tests

# .PHONY: docs
# docs: install
# 	$(RUN) typer src/cat_merge/cli.py utils docs --name cat-merge --output docs/Usage/CLI.md

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf .pytest_cache
	rm -rf dist

.PHONY: lint
lint:
	$(RUN) flake8 --exit-zero --max-line-length 120 src tests/
	$(RUN) black --check --diff src tests
	$(RUN) isort --check-only --diff src tests

.PHONY: format
format:
	$(RUN) autoflake \
		--recursive \
		--remove-all-unused-imports \
		--remove-unused-variables \
		--ignore-init-module-imports \
		--in-place src tests
	$(RUN) isort src tests
	$(RUN) black src tests
