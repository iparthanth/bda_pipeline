# EpiScope - one-command build (ENG-1).  `make all` rebuilds public-data cache, synthetic feeds, the
# bronze/silver/gold lake, every analytics output and the dashboard data from a clean clone.
#
#   make setup                 create .venv and install the pinned requirements
#   make all                   setup + env + full pipeline (SF from config/episcope.yaml, default 1.0)
#   make all SF=0.25           same at a smaller scale factor
#   make serve                 start the dashboard at http://127.0.0.1:8765 and open it (signs in automatically;
#                              ARGS=--secure requires a role token instead)
#   make drop DROP=path/dir    ingest an incremental drop (e.g. the hidden 14-day test drop) and refresh everything
#   make incremental           ingest the generator's first future day (demo of ING-4) and refresh
#   make stream                near-real-time path (ING-6): Structured Streaming over data/stream_inbox
#   make test                  automated tests (quality rules, de-identification, optimiser, suppression, idempotency)
#   make scale                 scalability experiment SF in {0.25, 0.5, 1} (ENG-2)
#   make optimisations         Spark optimisation before/after experiment (ENG-3)
#   make report                build docs/report/EpiScope_report.pdf from the latest results

SHELL := /bin/bash
PY := .venv/bin/python
SF ?=
SFARG := $(if $(SF),--sf $(SF),)
# Spark 3.5 supports Java 8/11/17. Prefer a Java 17 install when the default java is newer.
JAVA17 := $(firstword $(wildcard /usr/lib/jvm/java-17-openjdk*) $(shell /usr/libexec/java_home -v 17 2>/dev/null))
ifneq ($(JAVA17),)
export JAVA_HOME := $(JAVA17)
export PATH := $(JAVA17)/bin:$(PATH)
endif
export PYTHONPATH := $(CURDIR)

.PHONY: setup env all pipeline serve drop incremental stream test scale optimisations report clean results

setup: .venv/.installed
.venv/.installed: requirements.txt
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt
	touch $@

env: setup
	$(PY) -m orchestration.init_env

all: setup env
	$(PY) -m orchestration.dag all $(SFARG) $(if $(OFFLINE),--offline,)

serve: setup env
	$(PY) -m serving.server $(ARGS)

drop: setup env
	@test -n "$(DROP)" || (echo "usage: make drop DROP=/path/to/drop_folder" && exit 2)
	$(PY) -m orchestration.dag drop --source "$(DROP)"

incremental: setup env
	$(PY) -m orchestration.dag drop --source "$$(ls -d data/drops/*/ | head -1)"

stream: setup env
	$(PY) -m pipeline.streaming.stream_alerts

test: setup env
	$(PY) -m pytest -q tests

scale: setup env
	$(PY) -m orchestration.scale_experiment

optimisations: setup env
	$(PY) -m pipeline.experiments.spark_optimisations

results:
	$(PY) -m orchestration.collect_results

report: results
	cd docs/report && latexmk -pdf -interaction=nonstopmode -halt-on-error EpiScope_report.tex >/dev/null && cp EpiScope_report.pdf ..

clean:
	rm -rf data/lake data/warehouse data/metastore_db data/spark-events data/landing/encounters data/landing/stock \
	       data/landing/facilities data/drops data/ground_truth data/results data/exports serving/out
