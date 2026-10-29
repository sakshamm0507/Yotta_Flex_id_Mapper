# ID mapping pipeline: one-command entry points.
#   make setup generate run test report     full local flow
#   make sample                             rebuild the committed evidence in output/sample_run
SHELL  := /bin/bash
PYTHON ?= python3
VENV   ?= .venv
PY     := $(VENV)/bin/python
IDMAP  := PYTHONPATH=src $(PY) -m idmap
N      ?= 100000
SEED   ?= 42
GEN    := data/generated
SAMPLE_DB   := output/sample_run/idmap.duckdb
SAMPLE_ARGS := --db $(SAMPLE_DB) --log-dir output/sample_run/logs
SAMPLE_RUN  := run --incoming data/sample --pattern '*batch*.csv' \
               --canonical data/sample/canonical_entity.csv --overrides data/sample/mapping_override.csv --allow-failures

.PHONY: help setup generate run status test report sample perf clean all

help:
	@echo "setup     create $(VENV) and install pinned dependencies"
	@echo "generate  synthetic data (N=$(N), SEED=$(SEED)) into $(GEN), staged into data/incoming"
	@echo "run       process every batch in data/incoming not yet SUCCEEDED"
	@echo "status    show recent pipeline runs"
	@echo "test      pytest -v with coverage -> output/test_results.txt, output/coverage.txt"
	@echo "report    reconciliation + quality report -> output/report.md (non-zero exit on a failed check)"
	@echo "sample    rebuild output/sample_run from the committed sample"
	@echo "perf      timings at 10k / 100k / 1M rows -> output/perf.md"
	@echo "clean     remove generated data, databases and logs"

setup:
	@$(PYTHON) -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required, found " + sys.version'
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -r requirements.txt

generate:
	$(PY) scripts/generate.py --n $(N) --seed $(SEED) --out $(GEN)
	mkdir -p data/incoming
	rm -f data/incoming/*.csv
	cp $(GEN)/*_batch*.csv data/incoming/

# broken_schema_batch.csv is designed to FAIL. --allow-failures keeps the exit code 0 so the
# demo chain continues; the failure is still recorded in pipeline_run and retried on the next run.
run:
	$(IDMAP) run --allow-failures

status:
	$(IDMAP) status

test:
	mkdir -p output
	set -o pipefail; PYTHONPATH=src $(PY) -m pytest -v -p no:cacheprovider --cov=idmap --cov-report= 2>&1 \
		| sed "s|$(CURDIR)|.|g" | tee output/test_results.txt
	$(PY) -m coverage report | tee output/coverage.txt
	@echo "Coverage summary:" >> output/test_results.txt
	@cat output/coverage.txt >> output/test_results.txt

report:
	$(IDMAP) report

sample:
	rm -rf output/sample_run
	$(IDMAP) $(SAMPLE_ARGS) $(SAMPLE_RUN)
	@echo "--- second pass: unchanged files are SKIPPED, the failed file is retried ---"
	$(IDMAP) $(SAMPLE_ARGS) $(SAMPLE_RUN)
	$(IDMAP) $(SAMPLE_ARGS) report --expected-dir data/expected --out output/sample_run/report.md
	$(IDMAP) $(SAMPLE_ARGS) export --out output/sample_run/tables
	$(IDMAP) $(SAMPLE_ARGS) status --limit 50 > output/sample_run/status.txt
	cat output/sample_run/logs/*.jsonl > output/sample_run/run_log.jsonl

perf:
	PYTHONPATH=src $(PY) scripts/perf.py

clean:
	rm -rf $(GEN) data/incoming output/logs .pytest_cache .coverage
	rm -f data/*.duckdb data/*.duckdb.wal

all: setup generate run test report
