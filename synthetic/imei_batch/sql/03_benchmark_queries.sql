-- Same batch statement as production, pointed at the synthetic tables.
-- Snapshot clauses are omitted: the lab tables have their own snapshots.
-- Default generator settings should return 8607 rows for the seven-day
-- statement and the per-day counts in manifest.json for a one-day statement.
-- Use a fresh output table for every run. Iceberg INSERT appends.

INSERT INTO ipdr_rep.ipdr_report_imei_baseline_test
SELECT
    a.imsi,
    a.imei,
    a.imei_14,
    a.msisdn,
    a.mac_address,
    a.source_ip,
    a.source_port,
    a.destination_ip,
    a.destination_port,
    a.translated_ip,
    a.translated_port,
    a.first_cell_id_name_location,
    a.session_start_date_time,
    a.session_end_date_time,
    cast(a.duration_in_sec AS STRING),
    a.data_volume_uplink,
    a.data_volume_downlink,
    a.pgw_ip_address,
    a.charging_id,
    a.access_point_name,
    a.pdp_address_ipv4,
    a.pdp_address_ipv6,
    a.cgi_id,
    a.rat,
    a.pdp_type,
    'N' AS tethering,
    'N' AS application_name,
    'NA' AS ipdr_filename,
    'NA' AS cgf_filename,
    'NA' AS cgnat_filename,
    a.cgi_id AS roaming_nw_ceid,
    '' AS first_cellid_name_address,
    'NA' AS subs_circle,
    'NA' AS subs_roam_circle,
    a.date_partition,
    b.incident_number,
    b.req_id
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI b
INNER JOIN ipdr_db.ipdr_cisco_oct_2026 a
    ON a.imei_14 = b.report_value
WHERE a.date_partition IN (
    '2026-10-17', '2026-10-16', '2026-10-15', '2026-10-14',
    '2026-10-13', '2026-10-12', '2026-10-11'
)
  AND b.num_type = 'IMEI'
  AND (
    a.session_start_date_time BETWEEN b.from_date AND b.to_date
    OR a.session_end_date_time BETWEEN b.from_date AND b.to_date
  );

-- One-day form used by the tuning report. Change only the date, and write
-- to a separate empty table when comparing plans.
-- INSERT INTO ipdr_rep.ipdr_report_imei_baseline_test
-- SELECT ... same projection ...
-- WHERE a.date_partition = '2026-10-11'
--   AND b.num_type = 'IMEI'
--   AND (a.session_start_date_time BETWEEN b.from_date AND b.to_date
--        OR a.session_end_date_time BETWEEN b.from_date AND b.to_date);

-- Request-side invariants. Expected: 152, 115, 152, 0, 0, 0, 36.
SELECT
    COUNT(*) AS request_rows,
    COUNT(DISTINCT report_value) AS distinct_imei_count,
    COUNT(DISTINCT req_id) AS distinct_request_count,
    SUM(CASE WHEN report_value IS NULL THEN 1 ELSE 0 END) AS null_imei_count,
    SUM(CASE WHEN from_date IS NULL OR to_date IS NULL THEN 1 ELSE 0 END)
        AS null_period_count,
    SUM(CASE WHEN from_date > to_date THEN 1 ELSE 0 END) AS invalid_period_count
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
WHERE num_type = 'IMEI';

SELECT COUNT(*) AS overlapping_request_count
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
WHERE num_type = 'IMEI'
  AND from_date < CAST('2026-10-18 00:00:00' AS TIMESTAMP)
  AND to_date >= CAST('2026-10-11 00:00:00' AS TIMESTAMP);
