-- Load synthetic Parquet into the lab Iceberg tables.
-- Kerberos is required. On ccycloud-1, run_kerberos.sh obtains
-- systest@QE-INFRA-AD.CLOUDERA.COM from /cdep/keytabs/systest.keytab,
-- copies the generator output to HDFS, and replaces __DATA_ROOT__:
--   ./run_kerberos.sh load /data/imei_synth
-- DATA_ROOT defaults to /tmp/imei_synth. Override it in the environment.
--
-- Timestamps are Parquet INT96 values so Impala keeps them as written.
-- After the load, this check should stay on 2026-10-11 and must not shift by
-- the cluster timezone offset:
--   SELECT MIN(session_start_date_time), MAX(session_start_date_time)
--   FROM ipdr_db.ipdr_cisco_oct_2026
--   WHERE date_partition = '2026-10-11';
--
-- Production column stats on the fact table were empty (NDV -1). Leave the
-- fact table that way for the first baseline. Compute stats only on the
-- 152-row request table.

DROP TABLE IF EXISTS ipdr_db.ipdr_cisco_oct_2026_stage;
CREATE EXTERNAL TABLE ipdr_db.ipdr_cisco_oct_2026_stage (
  imsi STRING,
  imei STRING,
  imei_14 STRING,
  msisdn STRING,
  mac_address STRING,
  source_ip STRING,
  source_port STRING,
  destination_ip STRING,
  destination_port STRING,
  translated_ip STRING,
  translated_port STRING,
  first_cell_id_name_location STRING,
  session_start_date_time TIMESTAMP,
  session_end_date_time TIMESTAMP,
  duration_in_sec STRING,
  data_volume_uplink STRING,
  data_volume_downlink STRING,
  pgw_ip_address STRING,
  charging_id STRING,
  access_point_name STRING,
  pdp_address_ipv4 STRING,
  pdp_address_ipv6 STRING,
  cgi_id STRING,
  rat STRING,
  pdp_type STRING,
  tethering STRING,
  application_name STRING,
  subs_circle STRING,
  date_partition DATE
)
STORED AS PARQUET
LOCATION '__DATA_ROOT__/ipdr_db/ipdr_cisco_oct_2026';

DROP TABLE IF EXISTS ipdr_rep.lgl_cgnat_request_split_imei_stage;
CREATE EXTERNAL TABLE ipdr_rep.lgl_cgnat_request_split_imei_stage (
  req_id STRING,
  rec_id STRING,
  request_status STRING,
  report_type STRING,
  num_type STRING,
  report_value STRING,
  from_date TIMESTAMP,
  to_date TIMESTAMP,
  from_dt DATE,
  to_dt DATE,
  circle_id STRING,
  log_date TIMESTAMP,
  filename STRING,
  ip_version STRING,
  public_ip_version STRING,
  public_port STRING,
  private_ip STRING,
  private_ip_version STRING,
  private_port STRING,
  mac_address STRING,
  public_ip STRING,
  destination_ip STRING,
  destination_port STRING,
  destination_ip_version STRING,
  incident_number STRING,
  refresh_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '__DATA_ROOT__/ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI';

INVALIDATE METADATA ipdr_db.ipdr_cisco_oct_2026_stage;
INVALIDATE METADATA ipdr_rep.lgl_cgnat_request_split_imei_stage;

-- Reload requires an empty Iceberg table. Drop and recreate the two lab
-- targets if this load is being repeated. Do not drop them on a cluster that
-- already holds production data under these names.
INSERT INTO ipdr_db.ipdr_cisco_oct_2026
SELECT * FROM ipdr_db.ipdr_cisco_oct_2026_stage;

INSERT INTO ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
SELECT * FROM ipdr_rep.lgl_cgnat_request_split_imei_stage;

COMPUTE STATS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;

-- Fact-table column stats are intentionally not computed.
