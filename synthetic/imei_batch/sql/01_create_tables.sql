-- Lab schema for the IMEI batch benchmark.
-- Database, table, and column names match production.
-- Run this only on the internal lab cluster. Do not run it against production.
-- Kerberos: systest@QE-INFRA-AD.CLOUDERA.COM via /cdep/keytabs/systest.keytab.
--   ./run_kerberos.sh ddl
--
-- Iceberg SHOW PARTITIONS prints date_partition as days since 1970-01-01.
-- 2026-10-11 is 20737. The values stored in the column are real DATE values.

CREATE DATABASE IF NOT EXISTS ipdr_db;
CREATE DATABASE IF NOT EXISTS ipdr_rep;

-- Source fact table. Production snapshot 5180117802424828212 is not present
-- in the lab. Query the loaded table directly.
CREATE TABLE IF NOT EXISTS ipdr_db.ipdr_cisco_oct_2026 (
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
PARTITIONED BY SPEC (date_partition)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='snappy',
  'write.parquet.row-group-size-bytes'='268435456',
  'write.target-file-size-bytes'='1073741824',
  'write.ordering.columns'='msisdn,first_cell_id_name_location,translated_ip',
  'write.ordering.strategy'='zorder'
);

-- Request driver. Production has 152 rows at snapshot 4865977338790686061.
-- The synthetic table keeps that cardinality. It is not partitioned.
CREATE TABLE IF NOT EXISTS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI (
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
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='snappy'
);

-- Production report target. Left empty. The benchmark writes to the test
-- tables below so repeated runs do not mix with this table.
CREATE TABLE IF NOT EXISTS ipdr_rep.ipdr_report_hour_requests_imei (
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
  ipdr_filename STRING,
  cgf_filename STRING,
  cgnat_filename STRING,
  roaming_nw_ceid STRING,
  first_cellid_name_address STRING,
  subs_circle STRING,
  subs_roam_circle STRING,
  date_partition DATE,
  incident_number STRING,
  req_id STRING
)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='snappy'
);

CREATE TABLE IF NOT EXISTS ipdr_rep.ipdr_report_imei_baseline_test (
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
  ipdr_filename STRING,
  cgf_filename STRING,
  cgnat_filename STRING,
  roaming_nw_ceid STRING,
  first_cellid_name_address STRING,
  subs_circle STRING,
  subs_roam_circle STRING,
  date_partition DATE,
  incident_number STRING,
  req_id STRING
)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS ipdr_rep.ipdr_report_imei_static_test (
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
  ipdr_filename STRING,
  cgf_filename STRING,
  cgnat_filename STRING,
  roaming_nw_ceid STRING,
  first_cellid_name_address STRING,
  subs_circle STRING,
  subs_roam_circle STRING,
  date_partition DATE,
  incident_number STRING,
  req_id STRING
)
STORED AS ICEBERG;
