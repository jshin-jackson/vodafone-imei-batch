#!/usr/bin/env bash
# Run the lab DDL, HDFS load, and benchmark on the Kerberos-enabled cluster.
# Execute this on one of the three cluster nodes. Do not run it on a laptop.
#   ccycloud-1.sbimf-azure.root.comops.site
#   ccycloud-2.sbimf-azure.root.comops.site
#   ccycloud-3.sbimf-azure.root.comops.site
# Impala connects to the node the script is running on, so the Kerberos
# service principal is impala/<that FQDN>. Set IMPALA_HOST to pick another.
#
# The ticket matches the cluster check:
#   kinit -kt /cdep/keytabs/systest.keytab systest
#   Default principal: systest@QE-INFRA-AD.CLOUDERA.COM
# Root's ticket cache in that check was FILE:/tmp/krb5cc_0. This script uses
# the default cache of the current user, which is that path when run as root.

set -euo pipefail

KEYTAB="${KEYTAB:-/cdep/keytabs/systest.keytab}"
PRINCIPAL="${PRINCIPAL:-systest}"
REALM="${REALM:-QE-INFRA-AD.CLOUDERA.COM}"
IMPALA_SERVICE="${IMPALA_SERVICE:-impala}"
IMPALA_SHELL="${IMPALA_SHELL:-impala-shell}"
DATA_ROOT="${DATA_ROOT:-/tmp/imei_synth}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-sbimf-azure.root.comops.site}"
CLUSTER_HOSTS=(
  "ccycloud-1.${CLUSTER_DOMAIN}"
  "ccycloud-2.${CLUSTER_DOMAIN}"
  "ccycloud-3.${CLUSTER_DOMAIN}"
)
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [local-output-dir]

  kinit       Obtain a ticket for ${PRINCIPAL}@${REALM}
  shell       Open an authenticated Impala shell
  ddl         Create ipdr_db and ipdr_rep tables
  put DIR     Copy generator output from DIR to HDFS \${DATA_ROOT}
  load DIR    kinit, put DIR, create tables, and load Parquet
  benchmark   Run the seven-day INSERT into the baseline test table

Environment:
  KEYTAB=${KEYTAB}
  PRINCIPAL=${PRINCIPAL}
  REALM=${REALM}
  IMPALA_HOST   default: this node's FQDN, else ${CLUSTER_HOSTS[0]}
  Hosts: ${CLUSTER_HOSTS[*]}
  DATA_ROOT=${DATA_ROOT}
  IMPALA_SSL=1  pass --ssl when the coordinator requires TLS
EOF
}

kinit_systest() {
  if [[ ! -r "$KEYTAB" ]]; then
    echo "Keytab is not readable: $KEYTAB" >&2
    exit 1
  fi
  kinit -kt "$KEYTAB" "$PRINCIPAL"
  echo "Ticket principal: ${PRINCIPAL}@${REALM}"
  klist
}

impala_host() {
  if [[ -n "${IMPALA_HOST:-}" ]]; then
    printf '%s\n' "$IMPALA_HOST"
    return
  fi
  local short fqdn host
  short=$(hostname -s 2>/dev/null || hostname)
  fqdn=$(hostname -f 2>/dev/null || true)
  for host in "${CLUSTER_HOSTS[@]}"; do
    if [[ "$fqdn" == "$host" || "$short" == "${host%%.*}" ]]; then
      printf '%s\n' "$host"
      return
    fi
  done
  printf '%s\n' "${CLUSTER_HOSTS[0]}"
}

impala() {
  local host
  host=$(impala_host)
  echo "Impala coordinator: ${host} (service ${IMPALA_SERVICE}/${host}@${REALM})"
  local -a args=(
    -i "$host"
    -k
    --kerberos_service_name="$IMPALA_SERVICE"
  )
  if [[ "${IMPALA_SSL:-0}" == "1" ]]; then
    args+=(--ssl)
  fi
  "$IMPALA_SHELL" "${args[@]}" "$@"
}

run_sql() {
  local sql=$1
  local rendered
  local status=0
  rendered=$(mktemp)
  sed "s|__DATA_ROOT__|${DATA_ROOT}|g" "$sql" >"$rendered"
  impala -f "$rendered" || status=$?
  rm -f "$rendered"
  return "$status"
}

put_output() {
  local local_dir=${1:?local generator output directory is required}
  if [[ ! -d "$local_dir/ipdr_db/ipdr_cisco_oct_2026" ]]; then
    echo "Missing $local_dir/ipdr_db/ipdr_cisco_oct_2026" >&2
    exit 1
  fi
  if [[ ! -d "$local_dir/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI" ]]; then
    echo "Missing $local_dir/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI" >&2
    exit 1
  fi
  hdfs dfs -mkdir -p \
    "$DATA_ROOT/ipdr_db/ipdr_cisco_oct_2026" \
    "$DATA_ROOT/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI"
  hdfs dfs -put -f "$local_dir/ipdr_db/ipdr_cisco_oct_2026/"*.parquet \
    "$DATA_ROOT/ipdr_db/ipdr_cisco_oct_2026/"
  hdfs dfs -put -f "$local_dir/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI/"*.parquet \
    "$DATA_ROOT/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI/"
}

command=${1:-}
case "$command" in
  kinit)
    kinit_systest
    ;;
  shell)
    kinit_systest
    impala
    ;;
  ddl)
    kinit_systest
    run_sql "$SCRIPT_DIR/sql/01_create_tables.sql"
    ;;
  put)
    kinit_systest
    put_output "${2:-}"
    ;;
  load)
    kinit_systest
    put_output "${2:-}"
    run_sql "$SCRIPT_DIR/sql/01_create_tables.sql"
    run_sql "$SCRIPT_DIR/sql/02_load_parquet.sql"
    ;;
  benchmark)
    kinit_systest
    run_sql "$SCRIPT_DIR/sql/03_benchmark_queries.sql"
    ;;
  *)
    usage
    exit 2
    ;;
esac
