# Python 없이 진행하는 IMEI 검증: SQL·HDFS·Cloudera Manager

## 시작 전에

이 가이드에서는 Python을 사용하지 않는다. SQL 파일을 impala-shell로 실행하고, HDFS 상태는 hdfs 명령으로 확인한다. NameNode RPC·File handle cache·HDFS Client 상태는 Cloudera Manager(CM)와 각 서비스 Web UI에서 본다.

**파일 Open 조사만 먼저 하려면 1~6단계까지 진행한다.** 이후 단계는 최종 보고서의 개선안 검증이다. 실제 클러스터 실행은 여기서 수행하지 않았다.

검증은 내부 클러스터에서만 수행한다. 제공 SQL은 Source·요청을 수정하지 않는다. 실행마다 새 테스트 출력 테이블을 만들며 DROP·TRUNCATE는 포함하지 않았다.

이미 합성데이터를 적재했다면 저장소의 `load`를 다시 실행하지 않는다. INSERT INTO는 기존 데이터에 추가하므로 중복될 수 있다.

## 1단계. 파일 준비와 접속

다운로드한 ZIP을 클러스터 노드에 복사해 푼다.

```bash
unzip imei_sql_validation.zip
cd sql_validation
mkdir -p logs
kinit -kt /cdep/keytabs/systest.keytab systest
klist
```

같은 Bash 터미널에 아래 함수를 한 번 붙여 넣는다. `impala`라고 입력할 때마다 접속 옵션을 자동으로 붙이는 단축 명령이다.

```bash
impala() {
  impala-shell \
    -i ccycloud-2.sbimf-azure.root.comops.site \
    -d default -k --ssl \
    --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
    "$@"
}

impala -q 'SELECT VERSION();'
```

**성공 기준:** 버전 문자열이 출력된다. Kerberos 또는 인증서 오류가 있으면 다음 단계로 넘어가지 않는다. 새 터미널을 열면 단축 함수를 다시 정의한다.

## 2단계. 환경·데이터를 확인하고 Snapshot ID 기록

```bash
impala -f 00_inspect.sql > logs/00_inspect.log 2>&1
less logs/00_inspect.log
```

확인할 항목:

| 항목 | 확인 내용 |
|---|---|
| 요청 집계 | 152행 / IMEI 115개 / req_id 152개 |
| NULL·역전 기간 | 기본 생성 데이터는 각각 0 |
| 7일과 겹치는 요청 | 36 |
| Source 자료형 | IMEI는 STRING, 날짜는 DATE, 세션 시각은 TIMESTAMP |
| Source 통계 | 첫 기준 실행 전에는 새 COMPUTE STATS를 실행하지 않음 |
| 적용 옵션 | MEM_LIMIT, MT_DOP, MAX_FS_WRITERS, Late materialization |

로그의 두 DESCRIBE FORMATTED 결과에서 **current-snapshot-id**를 찾는다. 첫 번째는 Source, 두 번째는 요청 테이블이다.

```text
Source:  ipdr_db.ipdr_cisco_oct_2026
Request: ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
```

아래 값은 설명용 자리표시자다. 랩에서 확인한 숫자로 바꾼다. 고객 운영 보고서의 Snapshot ID를 사용하지 않는다.

```bash
SRC_SNAPSHOT=여기에_Source_Snapshot_숫자
REQ_SNAPSHOT=여기에_요청_Snapshot_숫자
```

예를 들어 숫자가 123과 456이었다면 다음처럼 입력한다. 이 숫자를 그대로 사용하라는 뜻은 아니다.

```bash
# 예시 형식만 참고
# SRC_SNAPSHOT=123
# REQ_SNAPSHOT=456
```

테이블 관리자에게 이 두 Snapshot을 검증 종료까지 보존하도록 요청한다. 검증 중 재적재·Compaction·통계·Daemon 설정 변경을 피한다. Snapshot 고정은 테이블 데이터만 고정하며 통계·환경을 모두 고정하지는 않는다.

고정한 요청 Snapshot의 상태 확인:

```bash
impala --var "REQ_SNAPSHOT=$REQ_SNAPSHOT" \
  -f 01a_request_check.sql
```

**성공 기준:** 152 / 115 / 152이며 NULL·14자리 숫자가 아닌 값은 0이다. 다르면 적재 데이터부터 확인한다.

## 3단계. 테스트 실행 옵션 설정

아래는 운영 기준과 비교할 때의 예시다. **35GiB가 랩의 프로세스·Pool 한도 안에 들어오는지 먼저 확인한다.** 실행할 수 없는 작은 랩이면 MEM을 8g 등 실제 승인 가능한 값으로 바꾸고 그 사실을 기록한다.

```bash
TEST_DATE=2026-10-11
MEM=35g
DOP=5
WRITERS=0
LM=-1
IMEI_FILTER='1=1'
```

- `LM`은 SET ALL에서 확인한 실제 기준값을 사용한다. 이미 20이면 LM=20으로 시작한다.
- MAX_FS_WRITERS가 지원되지 않는다면 제공된 explain/run SQL의 해당 SET 한 줄을 제거하고 Writer 비교는 보류한다.
- 예시 날짜의 기대 출력은 1,435행이다. 다른 날짜나 생성 설정을 사용하면 manifest.json의 해당 기대값을 사용한다.
- IMEI_FILTER=1=1은 IMEI 정적 조건을 추가하지 않는 기준 실행이다.

반복할 접속 변수를 묶는 두 번째 단축 명령을 같은 터미널에 붙여 넣는다.

```bash
bench() {
  impala \
    --var "SRC_SNAPSHOT=$SRC_SNAPSHOT" \
    --var "REQ_SNAPSHOT=$REQ_SNAPSHOT" \
    --var "OUT_TABLE=$OUT_TABLE" \
    --var "TEST_DATE=$TEST_DATE" \
    --var "MEM=$MEM" \
    --var "DOP=$DOP" \
    --var "WRITERS=$WRITERS" \
    --var "LM=$LM" \
    --var "IMEI_FILTER=$IMEI_FILTER" \
    "$@"
}
```

이후에는 변수 값을 바꾸고 `bench -f SQL파일`을 실행하면 된다. 변수는 해당 터미널 안에서만 유지된다.

## 4단계. 새 출력 테이블 생성 → EXPLAIN → 기준 실행

```bash
OUT_TABLE=imei_validation.day_baseline_01

# 새 출력 테이블 생성
bench -f 02_create_output.sql > logs/baseline_create.log 2>&1
```

명령이 끝나면 로그에 오류가 없는지 확인한다.

```bash
less logs/baseline_create.log
```

기존 테이블이 있다는 오류가 나면 새 이름(`day_baseline_02` 등)을 사용한다. 기존 테이블에 반복 INSERT하지 않는다.

```bash
# 실행 계획만 확인
bench -f 03_explain_day.sql > logs/baseline_explain.log 2>&1
less logs/baseline_explain.log
```

확인 사항: 날짜 1개, 랩 Snapshot 2개, Broadcast Join, 예상 Scan·Writer 인스턴스 수, 메모리 예약 요구. IMEI_FILTER=1=1은 최적화 후 불필요한 조건으로 정리되는지 확인한다.

그다음 CM 관측을 시작하고 Query를 실행한다.

```bash
date -u
bench -f 04_run_day.sql > logs/baseline_profile.log 2>&1
date -u
```

SQL 파일에는 INSERT 직후 SUMMARY와 PROFILE이 포함돼 있다. Query ID와 Duration은 해당 로그에서 확인한다. SQL 오류가 있으면 이후 비교를 중단한다.

```bash
# Profile을 확보한 뒤 출력 상태를 별도로 확인
bench -f 07_check_output.sql > logs/baseline_output.log 2>&1
less logs/baseline_output.log
```

**성공 기준:** 기본 합성데이터의 10월 11일 결과는 1,435행. 파일 수·크기는 랩 Writer 구성에 따라 달라진다. 0행이면 성능을 평가하지 말고 입력·날짜·Join부터 확인한다.

## 5단계. 파일 Open 조사: CM과 HDFS로 확인

이 단계에서는 캐시·Handler 수를 변경하지 않는다. 위 Query 실행 시간대를 정확히 기록하고 다음 3가지를 같은 시간에 본다.

### 5-1. NameNode RPC

CM에서 **HDFS → Instances → 실제 Active NameNode → Charts/Web UI**로 이동한다. impala-shell 접속 호스트가 NameNode라고 가정하지 않는다.

| 지표 | 질문 |
|---|---|
| RPC Queue time | 요청이 처리 시작 전에 기다리는가? |
| RPC Processing time | 요청 처리 자체가 느린가? |
| Call queue length | 대기 요청이 쌓이는가? |
| RPC 요청량 | Query 중 요청이 급증하는가? |
| JVM GC·Pause | 지연 시간대와 GC가 겹치는가? |

실행 시작 1~2분 전부터 종료 후 1분까지 차트 시간 범위를 잡는다. 짧은 랩 Query는 5초 집계에 묻힐 수 있으므로 CM 수집 주기를 확인한다. 평균뿐 아니라 제공되는 최대·백분위와 짧은 급증도 확인한다.

- Open 증가와 Queue·대기열 증가가 겹치면 공유 RPC 적체 가설이 강해진다.
- Queue는 낮고 Processing·GC가 높으면 NameNode 처리 경로를 더 조사한다.
- NameNode가 정상인데 Open이 크면 Client·연결·캐시 경로를 조사한다.

### 5-2. File handle cache

CM의 **Impala → Configuration → Maximum Cached File Handles**에서 현재 값을 기록한다. 참여 Executor의 Web UI `/metrics`에서 다음을 확인한다.

```text
impala-server.io.mgr.cached-file-handles-miss-count
impala-server.io.mgr.num-cached-file-handles
```

실행 전후 Miss 증가량과 실행 중 점유 상태를 기록한다. 지표는 해당 impalad 전체 값일 수 있으므로 다른 Query 부하도 기록한다. 공식적으로 제공되는 축출 지표가 없다면 점유·Miss와 반복 실행의 변화로 판단하고 축출을 확정하지 않는다.

- 첫 실행 Miss가 많고 다음 실행에서 감소: 초기 Open·재사용 효과 가능성.
- 점유가 한도에 근접하고 재사용 시에도 Miss가 많음: 용량·만료·동시 핸들 요구 조사.
- 점유가 낮아도 Miss가 많음: 캐시 부족으로 바로 단정하지 않는다.

**File handle cache는 HDFS 데이터 캐시와 다르다. `hdfs cacheadmin`으로 이 핸들 캐시를 조사하거나 조정하는 것은 아니다.**

### 5-3. HDFS Client

참여 Executor의 Impala Web UI에서 다음을 확인한다.

- `/hadoop-varz`: 실제 적용 Hadoop 설정.
- `/jmx`: JVM 메모리·GC.
- 제공되는 JVM Thread dump 화면: 지연 중 5~10초 간격으로 3회 확보.
- impalad/Java 로그: Timeout, 연결 재시도, 인증 실패, GC 경고.

같은 활성 Open 작업이 Lock·RPC 경로에서 반복 대기하는지 본다. 일반적인 유휴 Socket 대기 스레드 하나만으로 장애를 판단하지 않는다. Web UI에서 Thread dump 기능이 없다면 CM 진단 수집을 사용한다. impalad에 jstack/jcmd Attach가 항상 된다고 가정하지 않는다.

### 5-4. HDFS 명령: 상태와 실제 파일 배치 확인

```bash
hdfs dfsadmin -report > logs/hdfs_report.txt
hdfs getconf -nnRpcAddresses > logs/namenode_rpc_addresses.txt
```

`DESCRIBE FORMATTED`의 Source **Location**을 복사한다. 아래 경로는 실제 확인한 Location으로 교체한다.

```bash
SOURCE_LOCATION='hdfs://실제_네임서비스/실제_Source_경로'
hdfs dfs -count -h "$SOURCE_LOCATION" > logs/source_path_count.txt
hdfs dfs -ls "$SOURCE_LOCATION" > logs/source_path_listing.txt
```

- 이 경로의 파일 수에는 Metadata·과거 Snapshot 파일도 포함될 수 있다.
- 현재 Snapshot의 읽기 파일 수는 00_inspect.sql의 Iceberg `.files` 집계를 기준으로 한다.
- 큰 경로의 상세 fsck는 대표 하위 경로부터 사용한다.

```bash
# 실제 데이터 파일이 있는 대표 하위 경로로 교체
SAMPLE_DATA_PATH='hdfs://실제_네임서비스/실제_대표_데이터_하위경로'
hdfs fsck "$SAMPLE_DATA_PATH" -files -blocks -locations \
  > logs/sample_block_locations.txt
```

여기서는 읽기 전용 옵션만 사용한다. `-delete`, `-move`, Balancer는 실행하지 않는다. 이 명령은 블록 상태·위치 확인용이지 Open latency를 직접 측정하지 않는다. **SQL과 hdfs 명령만으로 NameNode RPC·Client Lock 지연을 확정할 수 없으므로 CM 관측이 필요하다.**

## 6단계. 같은 입력으로 반복하여 Open 원인을 좁힌다

설정을 그대로 두고 출력 이름만 바꾼다.

```bash
OUT_TABLE=imei_validation.day_baseline_02
bench -f 02_create_output.sql > logs/baseline2_create.log 2>&1
# 위 로그가 정상인지 확인한 후 실행
bench -f 04_run_day.sql > logs/baseline2_profile.log 2>&1
```

정합성 비교:

```bash
impala \
  --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_baseline_02 \
  -f 06_compare_exact.sql > logs/baseline_repeat_compare.log 2>&1
```

**통과값:** `mismatched_value_groups=0`, `differing_row_multiplicity=0`. 37개 출력 컬럼의 값과 중복 횟수를 비교하며 NULL도 같은 그룹으로 처리한다.

같은 데이터에서 Query 시간·Open·Cache miss와 CM 지표를 비교한다. 반복 실행은 OS·Metadata 캐시도 따뜻해질 수 있으므로 빨라졌다는 사실만으로 핸들 캐시 효과라고 확정하지 않는다.

| 관측 결과 | 해석 |
|---|---|
| Open 지연 + NameNode Queue 증가 | RPC 적체 가능성 |
| Open 지연 + NameNode GC·Processing 증가 | NameNode 처리 지연 가능성 |
| NameNode 정상 + Client 반복 Lock 대기 | Client 경합 가능성 |
| Miss·Open이 반복 실행에서 함께 감소 | 초기 접근·핸들 재사용 영향 가능성 |
| 작은 랩에서 지연이 재현되지 않음 | 정상 동작을 기록. 운영 가설이 틀렸다고 결론 내리지 않음 |

## 7단계. 메모리만 8GiB로 변경

35GiB 기준이 랩에서 가능한 경우에 수행한다. 기준이 이미 8GiB라면 중복 비교이므로 생략한다.

```bash
MEM=8g
OUT_TABLE=imei_validation.day_mem8_01
bench -f 02_create_output.sql > logs/mem8_create.log 2>&1
bench -f 03_explain_day.sql > logs/mem8_explain.log 2>&1
# 두 로그가 정상인지 확인한 후 실행
bench -f 04_run_day.sql > logs/mem8_profile.log 2>&1

impala \
  --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_mem8_01 \
  -f 06_compare_exact.sql > logs/mem8_compare.log 2>&1
```

통과 기준: 결과 차이 0/0, OOM·새 Spill 없음, 실제 적용 한도·Admission 산정값 감소, 의미 있는 시간 악화 없음. Pool Clamp가 값을 바꾸는지 확인한다. 동시 실행 상한은 유지한다.

다음 Matrix 전에 공통 메모리 기준을 다시 정한다. 아래 예시는 처음 기준값으로 되돌린 경우다.

```bash
MEM=35g
# 처음 기준이 8g였다면 8g를 유지한다.
```

## 8단계. Snapshot에서 정적 IMEI 목록 생성

```bash
impala -B --var "REQ_SNAPSHOT=$REQ_SNAPSHOT" \
  -f 01_imei_list.sql -o imei_in_list.tsv

head -c 120 imei_in_list.tsv
tail -c 80 imei_in_list.tsv
wc -c imei_in_list.tsv
```

파일 내용은 한 줄의 `'14자리값', '14자리값', ...`여야 한다. 헤더·행 수·Profile·표 테두리가 섞이면 사용하지 않는다. 14자리 115개라면 목록 자체는 2,068바이트이며 줄바꿈 포함 시 2,069바이트일 수 있다. Client 표시·내보내기 설정을 확인한다.

위 파일은 고정 요청 Snapshot에서 생성했기 때문에 생성기 디렉터리의 오래된 목록을 그대로 재사용하는 것보다 안전하다. 명령의 성공 여부와 앞뒤 작은따옴표·값을 확인한다.

정적 조건 변수 생성:

```bash
STATIC_IMEI_FILTER="a.imei_14 IN ($(cat imei_in_list.tsv))"
```

숫자 IMEI만 허용되는지 2단계의 검사에서 이미 확인해야 한다. 이 변수는 SQL 조건으로 전달하며 임의의 쉘 코드를 실행하는 용도가 아니다.

## 9단계. 정적 IN과 Late materialization 비교

실제 기준 LM이 -1인 경우만 다음 네 가지를 구성한다.

| Case | IMEI_FILTER | LM |
|---|---|---:|
| A | 1=1 | -1 |
| B | 정적 IMEI 목록 | -1 |
| C | 1=1 | 20 |
| D | 정적 IMEI 목록 | 20 |

기존 baseline_01이 A와 모든 옵션이 같으면 A는 재사용한다.

### B 실행

```bash
LM=-1
IMEI_FILTER="$STATIC_IMEI_FILTER"
OUT_TABLE=imei_validation.day_b_01
bench -f 02_create_output.sql > logs/b_create.log 2>&1
bench -f 03_explain_day.sql > logs/b_explain.log 2>&1
# 로그 확인 후
bench -f 04_run_day.sql > logs/b_profile.log 2>&1
```

### C 실행

```bash
LM=20
IMEI_FILTER='1=1'
OUT_TABLE=imei_validation.day_c_01
bench -f 02_create_output.sql > logs/c_create.log 2>&1
bench -f 03_explain_day.sql > logs/c_explain.log 2>&1
# 로그 확인 후
bench -f 04_run_day.sql > logs/c_profile.log 2>&1
```

### D 실행

```bash
LM=20
IMEI_FILTER="$STATIC_IMEI_FILTER"
OUT_TABLE=imei_validation.day_d_01
bench -f 02_create_output.sql > logs/d_create.log 2>&1
bench -f 03_explain_day.sql > logs/d_explain.log 2>&1
# 로그 확인 후
bench -f 04_run_day.sql > logs/d_profile.log 2>&1
```

각각 원본 결과와 비교한다.

```bash
impala --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_b_01 -f 06_compare_exact.sql
impala --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_c_01 -f 06_compare_exact.sql
impala --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_d_01 -f 06_compare_exact.sql
```

이미 LM이 활성화됐다면 일부러 끄지 않는다. 현재 LM에서 IMEI_FILTER=1=1과 정적 조건 두 가지만 먼저 비교한다.

**판정:** 0/0 정합성을 통과해야 한다. A→B는 정적 조건, A→C는 LM, B→D는 정적 조건이 있을 때 LM의 추가 효과다. 이 합성데이터의 scattered 배치는 Min/Max를 넓히도록 설계돼 있어 정적 IN만으로 읽기가 크게 줄지 않을 수 있다.

## 10단계. Writer와 MT_DOP는 각각 별도 시험

먼저 원본 필터·원래 LM·공통 MEM·DOP를 복원한다. Writer만 변경한다.

```bash
IMEI_FILTER='1=1'
LM=-1
DOP=5
WRITERS=8
OUT_TABLE=imei_validation.day_writer8_01
bench -f 02_create_output.sql > logs/writer8_create.log 2>&1
bench -f 03_explain_day.sql > logs/writer8_explain.log 2>&1
```

원래 LM이 20이었다면 위 LM=-1 대신 20을 사용한다. 계획에서 Writer 인스턴스만 제한되고 Source Scan 병렬성이 유지되는지 확인한다. 반영되지 않으면 효과가 확인된 것이 아니다.

```bash
bench -f 04_run_day.sql > logs/writer8_profile.log 2>&1
bench -f 07_check_output.sql > logs/writer8_output.log 2>&1
impala --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_writer8_01 -f 06_compare_exact.sql
```

MT_DOP만 변경하려면 Writer 기준값으로 복원한다.

```bash
WRITERS=0
DOP=2
OUT_TABLE=imei_validation.day_dop2_01
bench -f 02_create_output.sql > logs/dop2_create.log 2>&1
bench -f 03_explain_day.sql > logs/dop2_explain.log 2>&1
# 로그 확인 후
bench -f 04_run_day.sql > logs/dop2_profile.log 2>&1
impala --var TABLE_A=imei_validation.day_baseline_01 \
  --var TABLE_B=imei_validation.day_dop2_01 -f 06_compare_exact.sql
```

Writer 수는 파일 수를 보장하지 않는다. 실제 생성 파일 수·크기·Sink·Exchange·Duration을 함께 확인한다. 작은 랩에서 운영의 929파일 현상이 재현되지 않을 수도 있다.

## 11단계. 최종 후보만 전체 7일 확인

7일 기준 결과를 새로 만들 경우 원래 옵션을 복원한다. 아래는 최초 기준이 35g/5/0/-1이었던 예시다.

```bash
MEM=35g
DOP=5
WRITERS=0
LM=-1
IMEI_FILTER='1=1'
OUT_TABLE=imei_validation.full_baseline_01
bench -f 02_create_output.sql > logs/full_baseline_create.log 2>&1
# 생성 로그 확인 후
bench -f 05_run_seven_days.sql > logs/full_baseline_profile.log 2>&1
```

최선 후보의 실제 검증된 값으로 변수들을 변경한다. 모든 옵션을 무조건 묶지 않는다. 아래는 B가 선택된 예시이며, MEM·DOP·WRITERS·LM을 그대로 두고 정적 IN만 적용한다.

```bash
IMEI_FILTER="$STATIC_IMEI_FILTER"
OUT_TABLE=imei_validation.full_candidate_01
bench -f 02_create_output.sql > logs/full_candidate_create.log 2>&1
# 생성 로그 확인 후
bench -f 05_run_seven_days.sql > logs/full_candidate_profile.log 2>&1

impala --var TABLE_A=imei_validation.full_baseline_01 \
  --var TABLE_B=imei_validation.full_candidate_01 -f 06_compare_exact.sql
bench -f 07_check_output.sql
```

기대값은 8,607행이고 비교 결과는 0/0이다. 같은 Snapshot의 완전한 7일 기준 결과가 이미 있으면 이를 TABLE_A로 재사용할 수 있다. 기존 Profile의 행 수만으로는 비교할 수 없다.

## 12단계. 결과 표에 기록

| 실행 | 출력 행 | 비교 차이 | Duration | BytesRead | Open·I/O 대기 | CPU | 출력 파일 |
|---|---:|---|---|---|---|---|---:|
| 기준 | 1,435 | 기준 | | | | | |
| 메모리 8g | 1,435 | 0/0 | | | | | |
| B 정적 IN | 1,435 | 0/0 | | | | | |
| C LM20 | 1,435 | 0/0 | | | | | |
| D 둘 다 | 1,435 | 0/0 | | | | | |
| Writer8 | 1,435 | 0/0 | | | | | |
| DOP2 | 1,435 | 0/0 | | | | | |
| 7일 최종 | 8,607 | 0/0 | | | | | |

Open·Read는 같은 평균 Fragment 범위로 비교하고 개별 Instance·전체 값을 섞어 더하지 않는다. 첫 실행·반복 실행·다른 Query 부하를 기록한다. 한 번의 미세한 차이를 성능 개선으로 확정하지 않는다.

**3GiB 랩 결과로 운영 5시간의 단축률을 예측하지 않는다.** SQL 의미·옵션·필터링 동작 확인과 운영 규모 병목 재현은 별개의 검증이다.

## 주요 참고 자료

- [합성데이터 README](https://github.com/jshin-jackson/vodafone-imei-batch/blob/49213c845d977453cd829fc04d43e3477b61d149/README.md)
- [적재 SQL: Stage Parquet를 Iceberg로 INSERT](https://github.com/jshin-jackson/vodafone-imei-batch/blob/49213c845d977453cd829fc04d43e3477b61d149/synthetic/imei_batch/sql/02_load_parquet.sql)
- [Impala-shell 변수 및 파일 실행](https://impala.apache.org/docs/build/html/topics/impala_impala_shell.html)
- [Cloudera File handle cache](https://docs.cloudera.com/runtime/7.3.1/impala-reference/topics/impala-scalability.html)
- [Hadoop RPC 지표 정의](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/Metrics.html)
