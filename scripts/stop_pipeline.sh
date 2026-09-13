```bash
#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=============================================="
echo " Parando E-commerce Big Data Pipeline"
echo "=============================================="

cd "${PROJECT_ROOT}"

echo "[1/3] Parando Spark..."
if [[ -x "${PROJECT_ROOT}/scripts/run_spark.sh" ]]; then
    pkill -f "spark-submit" 2>/dev/null || true
fi

echo "[2/3] Parando Flink..."
if command -v flink >/dev/null 2>&1; then
    flink cancelall 2>/dev/null || true
fi

echo "[3/3] Parando geração e Flume..."
pkill -f "src/data_generator/gerador.py" 2>/dev/null || true
pkill -f "flume-ng agent" 2>/dev/null || true

echo
echo "Pipeline parado."
```
