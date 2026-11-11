#!/usr/bin/env bash
# Run on the lab node. Configure actual Web UI/JMX endpoints in Cloudera Manager.
# Authentication and ports for Web UIs are independent of impala-shell TLS.
set -euo pipefail
: "${NN_JMX_URL:?Set the active NameNode JMX URL from Cloudera Manager}"
: "${IMPALA_METRICS_URL:?Set the executor metrics URL from Cloudera Manager}"
out="${MONITOR_OUT:-monitor_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
# Add --cacert /actual/ca.pem for HTTPS if required. Do not disable TLS checks.
# --negotiate -u : is for SPNEGO-enabled endpoints; remove it for unauthenticated lab endpoints.
curl_args=(--fail --silent --show-error --max-time 15 --negotiate -u :)
for i in $(seq 1 "${MONITOR_SAMPLES:-120}"); do
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  curl "${curl_args[@]}" "$NN_JMX_URL" > "$out/nn_${stamp}.json" || echo "NN collection failed: $stamp" >&2
  curl "${curl_args[@]}" "$IMPALA_METRICS_URL" > "$out/impala_${stamp}.json" || echo "Impala collection failed: $stamp" >&2
  sleep "${MONITOR_INTERVAL:-5}"
done
