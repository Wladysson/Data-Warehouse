PYTHON := python3
VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

PROJECT_ROOT := $(shell pwd)

.PHONY: help install install-dev test coverage lint format \
        generator flume flink spark pipeline \
        check-hdfs check-hbase check-hive check-flink check-spark \
        healthcheck cleanup stop

help:
	@echo "Comandos disponíveis:"
	@echo "  make install        - instala dependências de produção"
	@echo "  make install-dev    - instala dependências de desenvolvimento"
	@echo "  make test           - executa os testes"
	@echo "  make coverage       - executa testes com cobertura"
	@echo "  make lint           - executa análise estática"
	@echo "  make format         - aplica formatação com Ruff"
	@echo "  make generator      - inicia o gerador de eventos"
	@echo "  make flume          - inicia o agente Flume"
	@echo "  make flink          - executa o job Flink"
	@echo "  make spark          - executa o job Spark"
	@echo "  make pipeline       - inicia o pipeline completo"
	@echo "  make stop           - para o pipeline"
	@echo "  make healthcheck    - verifica todos os serviços"
	@echo "  make cleanup        - encerra e limpa os containers"

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

test:
	$(PYTEST)

coverage:
	$(PYTEST) --cov=src --cov-report=term-missing --cov-report=html

lint:
	$(VENV)/bin/ruff check src tests

format:
	$(VENV)/bin/ruff format src tests

generator:
	./scripts/run_generator.sh

flume:
	./scripts/run_flume.sh

flink:
	./scripts/run_flink.sh

spark:
	./scripts/run_spark.sh

pipeline:
	./scripts/run_pipeline.sh

stop:
	./scripts/stop_pipeline.sh

check-hdfs:
	./scripts/check_hdfs.sh

check-hbase:
	./scripts/check_hbase.sh

check-hive:
	./scripts/check_hive.sh

check-flink:
	./scripts/check_flink.sh

check-spark:
	./scripts/check_spark.sh

healthcheck:
	./scripts/healthcheck.sh

cleanup:
	./scripts/cleanup.sh