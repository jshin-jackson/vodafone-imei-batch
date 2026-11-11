SELECT COUNT(*) AS expected_152,COUNT(DISTINCT report_value) AS expected_115,
 COUNT(DISTINCT req_id) AS expected_152_requests,
 SUM(CASE WHEN report_value IS NULL THEN 1 ELSE 0 END) AS null_imeis,
 SUM(CASE WHEN report_value IS NOT NULL AND NOT (report_value RLIKE '^[0-9]{14}$') THEN 1 ELSE 0 END) AS non_14_digit_imeis
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
FOR SYSTEM_VERSION AS OF ${var:REQ_SNAPSHOT}
WHERE num_type='IMEI';
