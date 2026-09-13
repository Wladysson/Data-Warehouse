#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "================================================"
echo " E-commerce Big Data Pipeline - Health Check"
echo "================================================"

FAILED=0

run_check() {
    local name="$1"
    local script="$2"

    echo
    echo "----------------------------------------------"
    echo " Verificando: ${name}"
    echo "----------------------------------------------"

    if [[ ! -x "${PROJECT_ROOT}/scripts/${script}" ]]; then
        echo "Aviso: ${script} não está executável."
        FAILED=1
        return
    fi

    if "${PROJECT_ROOT}/scripts/${script}"; then
        echo "OK: ${name}"
    else
        echo "FALHA: ${name}"
        FAILED=1
    fi
}

run_check "HDFS" "check_hdfs.sh"
run_check "HBase" "check_hbase.sh"
run_check "Hive" "check_hive.sh"
run_check "Flink" "check_flink.sh"
run_check "Spark" "check_spark.sh"

echo
echo "================================================"

if [[ "${FAILED}" -eq 0 ]]; then
    echo " Todos os componentes estão operacionais."
    echo "================================================"
    exit 0
fi

echo " Um ou mais componentes apresentaram falha."
echo "================================================"
exit 1