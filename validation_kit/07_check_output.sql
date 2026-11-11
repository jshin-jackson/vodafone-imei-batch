SELECT COUNT(*) AS output_rows FROM ${var:OUT_TABLE};
SELECT req_id,COUNT(*) AS output_rows FROM ${var:OUT_TABLE} GROUP BY req_id ORDER BY req_id;
SELECT COUNT(*) AS data_files,SUM(file_size_in_bytes) AS bytes,
 AVG(file_size_in_bytes) AS avg_bytes
FROM ${var:OUT_TABLE}.files WHERE content=0;
