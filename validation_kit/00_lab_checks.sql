-- Read-only checks against the currently loaded lab tables.
SELECT VERSION();
SET ALL;
SELECT COUNT(*) AS request_rows,
 COUNT(DISTINCT report_value) AS distinct_imeis,
 COUNT(DISTINCT req_id) AS distinct_requests,
 SUM(CASE WHEN report_value IS NULL THEN 1 ELSE 0 END) AS null_imeis,
 SUM(CASE WHEN from_date IS NULL OR to_date IS NULL THEN 1 ELSE 0 END) AS null_periods,
 SUM(CASE WHEN from_date>to_date THEN 1 ELSE 0 END) AS reversed_periods
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI WHERE num_type='IMEI';
SELECT COUNT(*) AS expected_36
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
WHERE num_type='IMEI'
 AND from_date<CAST('2026-10-18 00:00:00' AS TIMESTAMP)
 AND to_date>=CAST('2026-10-11 00:00:00' AS TIMESTAMP);
SHOW CREATE TABLE ipdr_db.ipdr_cisco_oct_2026;
SHOW TABLE STATS ipdr_db.ipdr_cisco_oct_2026;
SHOW COLUMN STATS ipdr_db.ipdr_cisco_oct_2026;
SHOW TABLE STATS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;
SHOW COLUMN STATS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;
SELECT spec_id, content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes,
 AVG(file_size_in_bytes) AS avg_bytes,
 SUM(CASE WHEN file_size_in_bytes<67108864 THEN 1 ELSE 0 END) AS files_lt_64m
FROM ipdr_db.ipdr_cisco_oct_2026.files GROUP BY spec_id,content;
SELECT f.file_path, f.record_count, f.file_size_in_bytes, f.sort_order_id,
 f.readable_metrics.imei_14.lower_bound AS imei_min,
 f.readable_metrics.imei_14.upper_bound AS imei_max
FROM ipdr_db.ipdr_cisco_oct_2026.files f
WHERE f.content=0 LIMIT 20;
