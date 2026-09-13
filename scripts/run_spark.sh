#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_JOB="${PROJECT_ROOT}/src/batch/spark_job.py"
SPARK_CONFIG="${PROJECT_ROOT}/configs/spark/spark-defaults.conf"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Spark Batch Job"
echo "=============================================="

if ! command -v spark-submit >/dev/null 2>&1; then
    echo "Erro: spark-submit não encontrado no PATH."
    exit 1
fi

if [[ ! -f "${SPARK_JOB}" ]]; then
    echo "Erro: job Spark não encontrado:"
    echo "${SPARK_JOB}"
    exit 1
fi

if [[ ! -f "${SPARK_CONFIG}" ]]; then
    echo "Erro: configuração do Spark não encontrada:"
    echo "${SPARK_CONFIG}"
    exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

echo "Job: ${SPARK_JOB}"
echo "Configuração: ${SPARK_CONFIG}"
echo "Iniciando processamento batch..."

exec spark-submit \
    --properties-file "${SPARK_CONFIG}" \
    "${SPARK_JOB}"