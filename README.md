# IMEI 배치 합성데이터

Impala IMEI 리포트 배치를 내부 클러스터에서 같은 SQL로 튜닝하기 위한 합성데이터입니다. 데이터베이스, 테이블, 컬럼 이름은 운영과 같습니다. 식별자는 합성값이며 고객 요청서의 IMEI는 넣지 않습니다.

생성기 위치는 `synthetic/imei_batch/`입니다.

## 맞춰 둔 조건

| 항목 | 값 |
|---|---:|
| `ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI` | 152행, IMEI 115개, `req_id` 152개 |
| IMEI별 요청 수 | 5건짜리 5개, 2건짜리 17개, 1건짜리 93개 |
| 요청 기간 | 2026-07-01 00:00:00 ~ 2026-11-04 21:22:32 |
| 2026-10-11~17과 겹치는 요청 | 36건 |
| 7일 구간에서 IMEI가 맞는 행 | 66,259 |
| 시간 조건까지 통과한 조인 결과 | 8,607 |
| `ipdr_db.ipdr_cisco_oct_2026` | 운영 파티션 행 수 비율, 기본 약 3GiB |

요청 테이블은 152행을 유지합니다. 팩트 테이블과 같은 바이트 비율로 줄이면 조인이 사라집니다. 팩트 테이블 컬럼 통계는 운영과 같이 비어 있습니다. 적재 스크립트는 요청 테이블만 `COMPUTE STATS`를 실행합니다.

SDV GaussianCopula는 세션의 지속시간, 트래픽량, 포트, RAT, APN, circle 분포를 학습합니다. 조인 키와 날짜 파티션 비율은 그 위에 고정합니다.

기본 레이아웃은 `scattered`입니다. `imei_14`가 흩어져 있어 row group min/max로 요청 IMEI를 건너뛰지 못합니다. 정렬 레이아웃을 비교할 때는 같은 시드로 `--layout imei-sorted`를 한 번 더 생성합니다.

## 1. 준비

```bash
cd synthetic/imei_batch
python3 -m pip install -r requirements.txt
```

Python 3.9 이상, `sdv` 2.x가 필요합니다. 출력 디렉터리는 비어 있는 새 경로여야 합니다. 이미 있으면 스크립트가 중단됩니다.

## 2. 백그라운드 생성

3GiB 생성은 터미널을 붙잡지 않도록 `--background`로 실행합니다. 로그와 PID는 출력 디렉터리 옆에 생깁니다.

```bash
python3 generate_synthetic_ipdr.py \
  --output-dir /data/imei_synth \
  --target-gib 3 \
  --layout scattered \
  --seed 20261011 \
  --background
```

확인:

```bash
cat /data/imei_synth.pid
tail -f /data/imei_synth.log
```

로그 마지막에 팩트 Parquet 크기와 `Join check passed: 8607 output rows`가 나오면 생성이 끝난 것입니다. 결과 디렉터리 구성은 다음과 같습니다.

```text
/data/imei_synth/
  ipdr_db/ipdr_cisco_oct_2026/*.parquet
  ipdr_rep/LGL_CGNAT_REQUEST_SPLIT_IMEI/part-00000.parquet
  manifest.json
  imei_in_list.txt
  requests.csv
  models/
```

`manifest.json`의 `output_rows_by_date`가 하루치 비교의 기대 건수입니다. 2026-10-11은 1,435행입니다. `imei_in_list.txt`는 14자리 IMEI 115개이며 길이는 2,068바이트입니다.

작은 동작 확인은 `--target-mib 64`를 사용합니다. 조인 결과 8,607행은 크기와 관계없이 유지됩니다.

## 3. 레이아웃 비교

```bash
python3 generate_synthetic_ipdr.py \
  --output-dir /data/imei_synth_sorted \
  --target-gib 3 \
  --layout imei-sorted \
  --seed 20261011 \
  --background
```

`--layout zorder`는 운영 테이블에 선언된 `msisdn`, `first_cell_id_name_location`, `translated_ip` 순서로 정렬합니다. `msisdn`은 IMEI와 1:1이라 이 정렬도 IMEI가 뭉칩니다.

## 4. 클러스터 적재

Kerberos가 켜져 있습니다. 아래 세 노드 중 하나에서 `synthetic/imei_batch`를 실행합니다.

- `ccycloud-1.sbimf-azure.root.comops.site`
- `ccycloud-2.sbimf-azure.root.comops.site`
- `ccycloud-3.sbimf-azure.root.comops.site`

스크립트는 Impala 코디네이터 `ccycloud-2.sbimf-azure.root.comops.site`에 Kerberos와 TLS로 접속합니다. 서비스 프린시펄은 `impala/ccycloud-2.sbimf-azure.root.comops.site`이고, CA 인증서는 `/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem`입니다. 다른 노드를 지정하려면 `IMPALA_HOST`를 넘깁니다.

```bash
kinit -kt /cdep/keytabs/systest.keytab systest
# principal: systest@QE-INFRA-AD.CLOUDERA.COM

export DATA_ROOT=/tmp/imei_synth
./run_kerberos.sh load /data/imei_synth
```

`load`가 티켓을 다시 받고, Parquet를 HDFS `${DATA_ROOT}`에 올린 다음 `ipdr_db`와 `ipdr_rep` 테이블을 만들고 Iceberg에 적재합니다. 접속은 `impala-shell -i ccycloud-2.sbimf-azure.root.comops.site -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem`과 같습니다.

이 적재는 랩 클러스터 전용입니다. 운영 데이터베이스에 실행하지 않습니다.

## 5. 벤치마크

7일 배치는 기본 설정에서 8,607행을 `ipdr_rep.ipdr_report_imei_baseline_test`에 넣습니다. Iceberg `INSERT`는 추가이므로, 다시 실행하기 전에 결과 테이블을 비우거나 테이블 이름을 바꿉니다.

```bash
./run_kerberos.sh benchmark
```

하루만 비교할 때는 `sql/03_benchmark_queries.sql`의 날짜 조건을 `a.date_partition = '2026-10-11'`로 바꾸고, 출력 테이블을 별도로 둡니다. 운영 스냅샷 ID는 랩 테이블에 없으므로 쿼리에 넣지 않습니다.

요청 테이블이 맞는지 확인하는 쿼리도 같은 파일에 있습니다. 기대값은 152, 115, 152, null 0, 기간 역전 0, 겹치는 요청 36입니다.
