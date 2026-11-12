# IMEI 배치: SQL만 변경하는 검증 가이드

## 목적과 범위

원본 SQL에 정적 `a.imei_14 IN (...)` 조건 하나를 추가하고 효과를 검증합니다.
테스트에서는 원본과 변경 쿼리에 모두 `SET MEM_LIMIT=2g;`를 적용합니다. 그 외 실행 옵션과 인프라 설정은 변경하지 않습니다.
`SET ALL`은 현재 옵션을 조회할 뿐 설정을 바꾸지 않습니다. 기존 운영 작업에 세션 설정이 있다면 메모리 한도만 이번 테스트 값으로 바꾸고, 나머지는 두 SQL 모두 동일하게 유지하세요.
`03_explain_day.sql`, `04_run_day.sql`, `05_run_seven_days.sql`의 첫 실행문에 `SET MEM_LIMIT=2g;`를 추가했습니다. 파일을 실행하는 각 세션에서 자동으로 적용됩니다.

이 값은 쿼리에 참여하는 각 노드의 쿼리 메모리 한도이며, 클러스터 전체 합계 2GB가 아닙니다. 리소스 풀 정책에 따라 실제 한도가 조정될 수 있으므로 실행 후 Profile의 적용 옵션과 메모리 한도를 확인하세요. 2GB에서 메모리 부족으로 실패하면 해당 실행은 성능 비교에서 제외하고, 양쪽 모두에 같은 한도를 적용하는 원칙을 유지하세요.

원본 SELECT 37개 컬럼, INNER JOIN, 요청의 중복, 시작/종료 시간의 OR 조건을 유지합니다.
DISTINCT는 IN 목록 생성에만 사용합니다. 실제 Join 요청 데이터는 중복 제거하지 않습니다.

## 1. 접속과 실행 함수

압축 파일을 풀어 이 README와 SQL 파일들이 있는 디렉터리로 이동하세요.
아래 명령은 SQL 파일을 impala-shell에 전달하는 용도이며 Python은 필요 없습니다.
기존 Kerberos 인증이 유효한 상태에서 실행합니다.

```bash
impala() {
  impala-shell \
    -i ccycloud-2.sbimf-azure.root.comops.site \
    -d default -k --ssl \
    --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
    "$@"
}

impala -f 00_snapshots.sql > 00_environment.log 2>&1
cat 00_environment.log
```

Source와 Request에서 테스트할 Snapshot ID를 각각 선택합니다. 이력 중 가장 최근 commit이 rollback 이후 현재 상태와 같다고 가정하지 마세요. 테스트 목적에 맞는 Snapshot을 선택하고 모든 비교에서 동일하게 사용합니다.
고객 환경의 과거 Snapshot ID를 내부 클러스터에 복사하지 마세요.
선택한 Snapshot과 그 파일이 테스트 도중 만료되지 않도록 테이블 관리 담당자와 협의하세요.

## 2. 비교 조건 지정

아래의 REPLACE 값을 실제 숫자로 교체한 후 실행합니다.

```bash
SRC_SNAPSHOT=REPLACE_WITH_SOURCE_SNAPSHOT_ID
REQ_SNAPSHOT=REPLACE_WITH_REQUEST_SNAPSHOT_ID
TEST_DATE=2026-10-11

bench() {
  impala \
    --var="SRC_SNAPSHOT=$SRC_SNAPSHOT" \
    --var="REQ_SNAPSHOT=$REQ_SNAPSHOT" \
    --var="TEST_DATE=$TEST_DATE" \
    --var="OUT_TABLE=$OUT_TABLE" \
    --var="IMEI_FILTER=$IMEI_FILTER" \
    "$@"
}

bench -f 01a_request_check.sql > 01_request_check.log 2>&1
cat 01_request_check.log
```

내부 생성 데이터의 기존 기대값은 요청 152행, IMEI 115개입니다.
실제 검증에서는 선택한 Snapshot의 조회 결과를 기준으로 삼으세요.
`non_14_digit_imeis`가 0이 아니면 목록 생성을 진행하지 말고 해당 값을 확인하세요.
목록 생성은 숫자 14자리로 구성된 문자열을 전제로 합니다. 앞자리의 0을 유지하기 위해 숫자 자료형으로 변환하지 않습니다.

## 3. 정적 IMEI 목록 생성하기

```bash
bench -B -o imei_in_list.tsv -f 01_imei_list.sql 2> 02_list_errors.log
cat 02_list_errors.log
cat imei_in_list.tsv
wc -c imei_in_list.tsv

STATIC_IMEI_FILTER="a.imei_14 IN ($(cat imei_in_list.tsv))"
```

목록이 비어 있지 않은지 확인하세요. 14자리 IMEI 115개라면 구분자를 포함해 2068 bytes이며, 마지막 줄바꿈이 있으면 파일은 2069 bytes입니다.
Impala에서는 `group_concat_max_len`을 설정하지 않습니다.
생성 목록과 실제 Join이 반드시 같은 Request Snapshot을 사용하는 것이 정합성 조건입니다.
운영에서도 요청 집합을 확정한 후 목록 생성과 본 쿼리 실행을 진행하세요.

## 4. 비어 있는 비교용 테이블 두 개 만들기

```bash
OUT_TABLE=imei_validation.sql_baseline_01
IMEI_FILTER='1=1'
bench -f 02_create_output.sql > 03_create_baseline.log 2>&1
cat 03_create_baseline.log

OUT_TABLE=imei_validation.sql_static_01
bench -f 02_create_output.sql > 04_create_static.log 2>&1
cat 04_create_static.log
```

원본 보고용 테이블과 같은 정의로 새로운 Iceberg 테이블을 만듭니다.
테스트용 데이터베이스와 테이블을 생성할 권한이 필요합니다. 권한이 없다면 관리자에게 같은 정의의 빈 테이블을 준비해 달라고 요청하세요.
테이블이 이미 존재하면 작업을 멈추고 사용하지 않은 새 이름을 선택하세요. 로그에 오류가 있어도 다음 단계로 진행하지 마세요.
원본 보고용 테이블에는 적재하지 않습니다. 필요하면 `SHOW CREATE TABLE`로 생성된 테이블의 정의를 확인하세요.

## 5. 실행 계획 비교하기

```bash
OUT_TABLE=imei_validation.sql_baseline_01
IMEI_FILTER='1=1'
bench -f 03_explain_day.sql > 05_plan_baseline.log 2>&1

OUT_TABLE=imei_validation.sql_static_01
IMEI_FILTER="$STATIC_IMEI_FILTER"
bench -f 03_explain_day.sql > 06_plan_static.log 2>&1

cat 05_plan_baseline.log
cat 06_plan_static.log
```

변경 쿼리의 원천 테이블 스캔에 `imei_14 IN (...)` 조건이 표시되는지 확인하세요.
선택된 파티션, 조인 방식, 예상 처리 행 수도 비교하세요.
계획상 파일 수가 같더라도 행 처리 비용이 줄어들 가능성은 있습니다.
실행 계획만으로 성능이 개선됐다고 판단하지 마세요.

## 6. 동일한 하루를 대상으로 실행하기

```bash
OUT_TABLE=imei_validation.sql_baseline_01
IMEI_FILTER='1=1'
bench -f 04_run_day.sql > 07_run_baseline.log 2>&1
cat 07_run_baseline.log

OUT_TABLE=imei_validation.sql_static_01
IMEI_FILTER="$STATIC_IMEI_FILTER"
bench -f 04_run_day.sql > 08_run_static.log 2>&1
cat 08_run_static.log
```

각 파일은 적재 후 `impala-shell`의 `SUMMARY`와 `PROFILE` 명령을 실행합니다.
이미 적재한 테이블에 재실행하면 결과가 추가됩니다. 반복 시험에서는 반드시 새로운 빈 테이블을 만들고 출력 대상을 바꾸세요.
37개 컬럼의 생성과 결과 저장까지 포함해 비교합니다. 결과를 사용자 화면으로 대량 전송하는 단순 조회나 `COUNT(*)`만 실행하는 쿼리로 대체하지 마세요.

## 7. 결과가 완전히 같은지 확인하기

```bash
impala \
  --var=TABLE_A=imei_validation.sql_baseline_01 \
  --var=TABLE_B=imei_validation.sql_static_01 \
  -f 06_compare_exact.sql > 09_compare.log 2>&1
cat 09_compare.log
```

합격 기준:

| 지표 | 합격 기준 |
|---|---:|
| mismatched_value_groups | 0 |
| differing_row_multiplicity | 0 |

37개 컬럼의 값과 행의 중복 횟수를 비교합니다. `NULL`도 같은 값 조합의 그룹으로 묶어 비교합니다.
전체 행 수만 같다고 합격으로 판단하지 마세요.
내부 생성 데이터의 기존 예상 결과는 하루 1,435행입니다. 원천 데이터나 생성 조건을 변경했다면 이 숫자를 고정된 기대값으로 사용하지 마세요.

## 8. 성능 개선 여부 판단하기

동일한 원천 스캔 노드와 동일한 집계 방식을 기준으로 다음 항목을 기록하세요.

| 지표 | 의미 |
|---|---|
| 전체 실행 시간 / 대기열 시간 | 실제 경과 시간과 실행 전 대기의 영향 |
| `BytesRead` | 실제 읽은 데이터량 |
| `RowsRead` / `RowsReturned` | 스캔에서 읽은 행과 출력한 행 |
| 파일·행 그룹 읽기 생략 관련 지표 | 읽지 않고 건너뛴 데이터 |
| `TotalRawHdfsOpenFileTime` | 파일 열기 경로의 누적 시간. 같은 방식의 집계값끼리 비교 |
| CPU 관련 지표 | 행 평가와 데이터 해석 등의 처리 비용 |
| 출력 행 수 / `FilesCreated` | 결과와 생성 파일 수의 변화 |

파일 열기 누적 시간을 전체 실행 시간으로 해석하지 마세요.
나중에 실행한 쿼리는 캐시 때문에 유리할 수 있습니다. 첫 비교에서 개선이 보이면 같은 하루 범위에서 순서를 뒤집어 다시 비교하세요.
예: 원본 A1 → 변경 B1, 다음에는 변경 B2 → 원본 A2. 각 실행에 새로운 빈 출력 테이블을 사용하세요.
동시 부하나 대기열 시간이 크게 다른 시험은 SQL 변경의 효과로 단정하지 마세요.
필요하면 비교를 추가하고 중앙값과 변동 폭을 확인하세요. 효과가 보이지 않으면 무조건 시험 횟수를 늘리지 말고 먼저 실행 정보를 검토하세요.

개선율(%) = (원본 실행 시간 - 변경 실행 시간) / 원본 실행 시간 × 100
예를 들어 100초에서 80초로 줄면 20% 단축입니다. 대기열 시간 차이는 별도로 설명하세요.
작은 차이가 평소 변동 폭 안에 있으면 ‘개선 효과 미확인’으로 판단합니다.

## 9. 전체 7일 범위 확인 후 운영에 적용하기

하루 범위에서 결과 일치와 반복 가능한 성능 개선을 확인한 경우에만 진행하세요.
새로운 빈 테이블을 준비하고 같은 절차로 `05_run_seven_days.sql`을 사용해 원본과 변경 쿼리를 비교하세요.
이 파일의 조회 기간은 2026년 10월 11일부터 17일까지입니다. 대상 기간이 다르면 양쪽 쿼리를 동일하게 수정하세요.
같은 스냅샷을 사용한 비교 가능한 전체 기간의 원본 결과와 실행 정보가 있다면 재사용할 수 있습니다. 없다면 원본 측정도 준비하세요. 변경 쿼리 한 번의 실행만으로 전체 기간의 개선을 단정하지 마세요.
`06_compare_exact.sql`로 전체 기간의 결과도 일치하는지 확인하세요.

운영에서는 시험용 하루 조건을 남기지 말고 원래 조회 기간으로 복원하세요.
원본 출력 컬럼, 조인, 시간의 `OR` 조건, 요청 중복과 기존 운영 세션 설정은 유지하고, 동일한 요청 집합에서 생성한 `IN` 조건만 추가하세요. 이번 테스트의 2GB 한도를 운영에 그대로 적용할지는 별도로 판단하세요.
요청 집합이 바뀔 때마다 IMEI 목록을 다시 생성하세요. 이전 목록을 재사용하면 결과가 누락될 수 있습니다.

## 변경하면 안 되는 항목

- 실제 조인에 사용하는 요청 데이터를 `DISTINCT`로 중복 제거하지 마세요.
- 시간 조건의 `OR`를 `AND`로 바꾸지 마세요.
- 시간 조건을 단순히 두 개의 `UNION ALL`로 분리하지 마세요. 두 조건을 모두 만족하는 행이 중복될 수 있습니다.
- `date_partition`과 세션 시각의 관계를 확인하지 않은 채 날짜·시간 조건을 추가해 조회 대상을 줄이지 마세요.
- IMEI를 숫자로 변환해 앞자리의 0을 없애거나 값을 누락하지 마세요. 요청이 변경되면 이전 목록을 재사용하지 마세요.
- `LIMIT`이나 `COUNT(*)`만 사용하는 시험으로 전체 적재 성능을 판단하지 마세요.

정적 `IN` 조건으로 개선되지 않으면 변경 후 실행 정보를 확인한 뒤 다음 SQL 변경을 결정하세요.
이미 사용하는 브로드캐스트 조인에 지시문을 추가하거나, 공통 테이블 식으로 바꾸거나, 표기상 `SELECT *`를 제거하는 것만으로 큰 개선을 기대하지 마세요.

## 참고 문서

- [Impala의 Iceberg 스냅샷 지정 조회](https://impala.apache.org/docs/build/html/topics/impala_iceberg.html)
- [Impala GROUP_CONCAT](https://impala.apache.org/docs/build/html/topics/impala_group_concat.html)

내부 클러스터에서 실제 실행하지는 않았습니다. 각 단계의 실행 계획과 오류 로그를 확인하면서 진행하세요.
