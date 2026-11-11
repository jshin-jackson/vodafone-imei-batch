SELECT VERSION();
SET ALL;
DESCRIBE FORMATTED ipdr_db.ipdr_cisco_oct_2026;
DESCRIBE FORMATTED ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;
SHOW TABLE STATS ipdr_db.ipdr_cisco_oct_2026;
SHOW COLUMN STATS ipdr_db.ipdr_cisco_oct_2026;
SHOW COLUMN STATS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;
SELECT COUNT(*) AS request_rows,
 COUNT(DISTINCT report_value) AS distinct_imeis,
 COUNT(DISTINCT req_id) AS distinct_requests,
 SUM(CASE WHEN report_value IS NULL THEN 1 ELSE 0 END) AS null_imeis,
 SUM(CASE WHEN from_date IS NULL OR to_date IS NULL THEN 1 ELSE 0 END) AS null_periods,
 SUM(CASE WHEN from_date>to_date THEN 1 ELSE 0 END) AS invalid_periods
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI WHERE num_type='IMEI';
SELECT COUNT(*) AS overlapping_requests_expected_36
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI WHERE num_type='IMEI'
 AND from_date<CAST('2026-10-18 00:00:00' AS TIMESTAMP)
 AND to_date>=CAST('2026-10-11 00:00:00' AS TIMESTAMP);
SELECT spec_id, content, COUNT(*) AS file_count,
 SUM(file_size_in_bytes) AS total_bytes, AVG(file_size_in_bytes) AS avg_bytes
FROM ipdr_db.ipdr_cisco_oct_2026.files GROUP BY spec_id,content;
