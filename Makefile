ifeq ($(OS),Windows_NT)
BIN = $(abspath .venv-dbt/Scripts)
DBT ?= $(BIN)/dbt.exe
SQLFLUFF ?= $(BIN)/sqlfluff.exe
PYTHON ?= $(BIN)/python.exe
else
BIN = $(abspath .venv-dbt/bin)
DBT ?= $(BIN)/dbt
SQLFLUFF ?= $(BIN)/sqlfluff
PYTHON ?= $(BIN)/python
endif
.PHONY: deps build test docs lint up load
up:
	docker compose -f compose.dbt.yaml up -d --wait
deps:
	cd dbt && "$(DBT)" deps --profiles-dir .
build:
	cd dbt && "$(DBT)" build --profiles-dir .
test:
	cd dbt && "$(DBT)" test --profiles-dir .
docs:
	cd dbt && "$(DBT)" docs generate --profiles-dir .
lint:
	cd dbt && "$(SQLFLUFF)" lint models tests --config .sqlfluff
load:
	"$(PYTHON)" scripts/load_dbt_data.py
