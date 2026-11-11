-- Export one field only. No header, no profile output.
SELECT GROUP_CONCAT(CONCAT(CHR(39), report_value, CHR(39)), ', ')
 AS imei_in_list
FROM (
 SELECT DISTINCT report_value
 FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
 FOR SYSTEM_VERSION AS OF ${var:REQ_SNAPSHOT}
 WHERE num_type='IMEI' AND report_value IS NOT NULL
) t;
