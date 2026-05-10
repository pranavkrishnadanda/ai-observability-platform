#!/usr/bin/env bash
# -----------------------------------------------------------------
# Load test runner for AI Observability Platform
# Usage: ./run_tests.sh [scenario] [host]
#   scenario: baseline | stress | mixed (default: baseline)
#   host:     API base URL (default: http://localhost:8000)
# -----------------------------------------------------------------

set -euo pipefail

HOST="${2:-http://localhost:8000}"
SCENARIO="${1:-baseline}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$(dirname "$0")/results/${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

LOCUST="python3.12 -m locust"
LOCUSTFILE="$(dirname "$0")/locustfile.py"

echo "========================================="
echo " AI Observability Platform — Load Tests"
echo " Scenario : $SCENARIO"
echo " Host     : $HOST"
echo " Results  : $RESULTS_DIR"
echo "========================================="

case "$SCENARIO" in

  baseline)
    echo ""
    echo "SCENARIO 1: Baseline Throughput"
    echo "  50 users | 10 spawn/s | 60s | target: 5,000+ req/min"
    $LOCUST -f "$LOCUSTFILE" \
      --headless \
      --users 50 \
      --spawn-rate 10 \
      --run-time 60s \
      --host "$HOST" \
      --csv "$RESULTS_DIR/baseline" \
      --html "$RESULTS_DIR/baseline_report.html" \
      --only-summary \
      2>&1 | tee "$RESULTS_DIR/baseline.log"
    ;;

  stress)
    echo ""
    echo "SCENARIO 2: Ingestion Stress"
    echo "  200 users | 20 spawn/s | 120s | target: 100,000+ events/min"
    $LOCUST -f "$LOCUSTFILE" \
      --headless \
      --users 200 \
      --spawn-rate 20 \
      --run-time 120s \
      --host "$HOST" \
      --csv "$RESULTS_DIR/stress" \
      --html "$RESULTS_DIR/stress_report.html" \
      --only-summary \
      --tags ingestion \
      2>&1 | tee "$RESULTS_DIR/stress.log"
    ;;

  mixed)
    echo ""
    echo "SCENARIO 3: Mixed Realistic Load"
    echo "  100 users | 10 spawn/s | 180s | target: p95 <100ms on queries"
    $LOCUST -f "$LOCUSTFILE" \
      --headless \
      --users 100 \
      --spawn-rate 10 \
      --run-time 180s \
      --host "$HOST" \
      --csv "$RESULTS_DIR/mixed" \
      --html "$RESULTS_DIR/mixed_report.html" \
      --only-summary \
      2>&1 | tee "$RESULTS_DIR/mixed.log"
    ;;

  all)
    "$0" baseline "$HOST"
    "$0" stress   "$HOST"
    "$0" mixed    "$HOST"
    ;;

  *)
    echo "Unknown scenario: $SCENARIO"
    echo "Usage: $0 [baseline|stress|mixed|all] [host]"
    exit 1
    ;;
esac

echo ""
echo "Results saved to: $RESULTS_DIR"
