-- 원본과 변경 쿼리에 동일한 테스트 메모리 한도 적용
SET MEM_LIMIT=2g;
INSERT INTO ${var:OUT_TABLE}
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
    cast(a.duration_in_sec as string),
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
    'N' as tethering,
     'N' as application_name,
    'NA' as ipdr_filename,
    'NA' cgf_filename,
    'NA' cgnat_filename,
    a.cgi_id as roaming_nw_ceid,
     '' as first_cellid_name_address,
     'NA' as subs_circle,
    'NA' as subs_roam_circle,
    a.date_partition,
     b.incident_number,
    b.req_id
FROM (
    SELECT * FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
    FOR SYSTEM_VERSION AS OF ${var:REQ_SNAPSHOT}
) b
INNER JOIN (
    SELECT * FROM ipdr_db.ipdr_cisco_oct_2026
    FOR SYSTEM_VERSION AS OF ${var:SRC_SNAPSHOT}
) a
    ON a.imei_14 = b.report_value
WHERE a.date_partition = DATE '${var:TEST_DATE}'
  AND b.num_type = 'IMEI'
  AND (session_start_date_time between b.from_date and b.to_date  OR session_end_date_time between b.from_date and b.to_date)
  AND (${var:IMEI_FILTER});
SUMMARY;
PROFILE;
