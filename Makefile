DBT = .venv-dbt/Scripts/dbt.exe
.PHONY: deps build test docs lint up
up:
	docker compose -f compose.dbt.yaml up -d --wait
deps:
	cd dbt && ../$(DBT) deps --profiles-dir .
build:
	cd dbt && ../$(DBT) build --profiles-dir .
test:
	cd dbt && ../$(DBT) test --profiles-dir .
docs:
	cd dbt && ../$(DBT) docs generate --profiles-dir .
lint:
	cd dbt && ../.venv-dbt/Scripts/sqlfluff.exe lint models tests --config .sqlfluff
