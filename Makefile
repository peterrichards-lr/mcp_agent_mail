.PHONY: help serve-http migrate lint lint-check typecheck test bench test-client verify guard-install guard-uninstall claims

PY=uv run
CLI=$(PY) python -m mcp_agent_mail.cli
# The agent-machine client's tests (also run on Windows by .github/workflows/windows-client.yml).
CLIENT_TESTS=tests/test_mail_wake.py tests/test_installer.py tests/test_file_attachments.py

help:  ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'

serve-http:  ## Run the HTTP server
	$(CLI) serve-http

migrate:  ## Apply database migrations
	$(CLI) migrate

lint:  ## ruff, applying fixes (read the diff before committing)
	$(PY) ruff check --fix --unsafe-fixes

lint-check:  ## ruff, changing nothing (what CI runs)
	$(PY) ruff check

typecheck:  ## ty
	uvx ty check

test:  ## The whole test suite, one worker per core
	$(PY) pytest -q -n auto --dist worksteal -m "not benchmark"

bench:  ## The timing benchmarks, one at a time, then CI's check of them against baselines.json
	RUN_BENCHMARKS=1 INSTRUMENTATION_ENABLED=true $(PY) pytest -q --no-cov -m benchmark tests/benchmarks/bench_*.py
	$(PY) pytest -q --no-cov -m ci_regression tests/benchmarks/test_ci_regression.py

test-client:  ## The agent-machine client's tests
	$(PY) pytest -q --no-cov -p no:cacheprovider $(CLIENT_TESTS)

verify: lint-check typecheck test  ## CI's lint, type and test steps: run before pushing

guard-install:
	$(CLI) guard install $(PROJECT) $(REPO)

guard-uninstall:
	$(CLI) guard uninstall $(REPO)

claims:
	$(CLI) claims list --active-only $(ACTIVE) $(PROJECT)


