# Vodafone IMEI Batch 내부 클러스터 검증 실행 가이드

## 1. 검증 목표와 전제

이 가이드는 최종 성능 보고서의 순서대로 **입력 확인 → Open 경로 관측 → 메모리 → 정적 IN/Late materialization → Writer·MT_DOP → 전체 7일**을 검증한다. 제공 스크립트는 별도 테스트 DB에 새 테이블만 만들며 Source·요청을 수정하거나 DROP/TRUNCATE하지 않는다.

**실행 장소:** 내부 클러스터 노드. 이 문서는 고객 운영 클러스터에 실행하지 않는다. 여기서 파일을 생성·로컬 테스트했지만 실제 클러스터 접속·원격 Query 실행은 수행하지 않았다.

### 저장소 확인 결과

확인한 코드 기준: `jshin-jackson/vodafone-imei-batch`, commit `49213c845d977453cd829fc04d43e3477b61d149`.

| 항목 | 기대값·상태 |
|---|---|
| Source | `ipdr_db.ipdr_cisco_oct_2026` |
| 요청 | `ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI` |
| 요청 행 / 고유 IMEI / 고유 req_id | 152 / 115 / 152 |
| 10월 11~17일과 기간이 겹치는 요청 | 36 |
| 7일 IMEI 일치 Source 행 | 66,259 |
| 7일 시간 조건까지 적용한 출력 | 8,607 |
| 2026-10-11 출력 | 1,435 (`manifest.json`을 최종 기준으로 사용) |
| 기본 생성 크기 | 약 3GiB, 운영 95.6TiB 읽기의 축소 복제는 아님 |
| Source 컬럼 통계 | 의도적으로 비어 있음 |
| 요청 통계 | 적재 스크립트에서 이미 COMPUTE STATS 실행 |

SDV는 지속시간·트래픽·포트 등의 분포를 만들고, Join key와 결과 행 수는 별도로 맞춘다. 크기를 바꿔도 8,607행을 유지하므로 데이터 확대 시 선택도까지 운영과 비례하는 것은 아니다.

**중요한 한계:** 3GiB·소수 노드의 랩은 SQL 정합성, 옵션 지원, pruning·CPU·Writer 동작을 검증하기 좋다. 운영의 수백만 Open, NameNode 부하, 188개 Executor와 17개 동시 Query를 그대로 재현하지 않는다. 랩에서 Open 지연이 없다고 운영의 NameNode 가설을 부정할 수 없고, 랩의 개선율을 운영 5시간에 곱하면 안 된다.

## 2. 도구 구성

- `validate.py`: 환경 확인, 랩 Snapshot·IMEI 고정, SQL 생성, 실행, 37컬럼 결과 비교, CSV 요약.
- `config.example.json`: 전달받은 Kerberos/TLS 접속 정보와 실제 테이블명이 반영된 설정 예시.
- `00_lab_checks.sql`: 현재 적재 데이터와 파일 통계 확인.
- `profile_fields.py`: 평균 Fragment와 전체 지표의 주요 행 추출. 누적 카운터 자동 합산은 하지 않음.
- `make_semantic_fixture.py`: NULL·경계·중복·날짜 불일치 검증용 별도 SQL 생성.
- `monitor.example.sh`: Web UI/JMX 시계열 수집 예시.
- `test_validate.py`: 로컬 생성·정합성 비교 로직 검증. Impala 실행 검증을 대신하지 않음.

Python 3.9+와 기존 `impala-shell`을 사용한다. Python 추가 패키지는 필요 없다.

## 3. 준비: 기존 적재를 반복하지 않는다

이미 적재가 끝났다면 저장소의 `load`를 다시 실행하지 않는다. `02_load_parquet.sql`은 INSERT INTO로 추가하므로 Source·요청이 중복될 수 있다. 저장소의 `benchmark`도 기본 결과 테이블에 추가하므로 반복 비교에는 사용하지 않는다.

압축 파일을 내부 노드에 복사해 푼 뒤:

```bash
unzip imei_validation_kit.zip
cd validation_kit
cp config.example.json config.json
kinit -kt /cdep/keytabs/systest.keytab systest
klist

impala-shell -i ccycloud-2.sbimf-azure.root.comops.site -d default \
  -k --ssl \
  --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
  -q 'SELECT VERSION();'
```

`config.json`의 `run_id`는 데이터셋·입력별 고유한 값으로 지정한다. 예: `scattered_3g_01`. 테이블명 길이를 고려해 짧게 쓴다. 접속 설정에는 비밀번호를 넣지 않는다. 티켓이 만료되면 갱신하고 새 실행 라벨로 재시도한다.

**메모리 사전 확인:** 예시의 35GiB는 운영 Query 설정을 비교하기 위한 값이다. 랩의 노드별 Impala 프로세스 한도와 Pool 예산보다 크면 그대로 실행하지 않는다. 운영값이 실행 가능한 환경에서만 35→8GiB를 비교한다. 작은 랩이라면 실제 가능한 기준값(예: 8GiB)으로 바꾸고, 기준이 이미 8이면 `mem8` 비교는 생략한다. 그 경우 6GiB는 별도 검증 후보이며 운영 35GiB Admission 현상을 재현했다고 보고하지 않는다.

`baseline_options`의 Late materialization 값도 실제 SET ALL 결과에 맞춘다. 이미 20이면 20으로 두고 `baseline` 대 `static_current`부터 비교한다. `MAX_FS_WRITERS=0`이 지원되지 않으면 기준 옵션에서 그 항목을 제거하고 Writer 시험은 지원 확인 후 수행한다. MT_DOP·Timezone·압축·파일 설정도 기록한다.

## 4. 0단계: 적재 및 SDV 품질 확인

```bash
impala-shell -i ccycloud-2.sbimf-azure.root.comops.site -d default \
  -k --ssl \
  --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
  -f 00_lab_checks.sql > 00_lab_checks.log 2>&1

python3 validate.py inspect
```

확인 사항:

1. 요청 집계가 `152,115,152,0,0,0`, 기간 겹침이 36인지 확인한다. 다르면 중복 적재·잘못된 경로·다른 생성 설정부터 점검한다.
2. Source는 DATE Partition, 시간 컬럼은 TIMESTAMP, 양쪽 IMEI는 STRING인지 확인한다. 숫자로 변환해 선행 0을 잃지 않는다.
3. 요청 컬럼 통계가 이미 있는지 확인한다. Source 통계는 첫 비교 전에 새로 계산하지 않는다. 통계 효과를 따로 볼 때만 다른 시험 단계로 분리한다.
4. 현재 Iceberg Data/Delete 파일 수·평균 크기·IMEI Min/Max를 기록한다. 입력 Parquet의 `manifest.json` 값과 적재 후 Iceberg 파일 수가 같을 필요는 없다.

생성기 결과의 기대값 확인:

```bash
python3 - <<'PY'
import json
m=json.load(open('/data/imei_synth/manifest.json'))
print('seed:',m['seed'])
print('source:',m['tables']['ipdr_db.ipdr_cisco_oct_2026'])
print('expected funnel:',m['funnel'])
print('input row groups:',m['row_group_stats'])
PY
```

### 실제 Iceberg 배치를 확인해야 하는 이유

저장소의 적재는 일반 Parquet Stage를 읽어 Iceberg에 INSERT한다. 따라서 생성기의 파일·Row group 경계와 정렬이 최종 Iceberg에서 그대로 유지된다고 보장할 수 없다. `scattered` 생성기는 Row group에 낮은·높은 sentinel IMEI를 넣어 Min/Max를 넓힌다. 정적 IN에서 Min/Max 기반 효과가 작아도 그 자체는 실패가 아니다. 최종 Iceberg 파일 Metadata와 실행 Profile로 판정한다.

`--layout zorder`는 생성기 내부의 hash 비트 interleave 구현이며 실제 엔진의 Z-order와 동일하다고 가정하지 않는다. MSISDN과 IMEI도 합성데이터에서 1:1 연결돼 있어 실제 업무의 시간별·다대다 관계를 재현하지 않는다.

## 5. 1단계: 랩 Snapshot 및 IMEI 고정

```bash
python3 validate.py freeze
cat artifacts/scattered_3g_01/frozen.json
python3 validate.py quality
```

예시는 `run_id=scattered_3g_01`일 때의 경로다. 기본값을 유지했다면 `artifacts/lab_20261111_01/`이다.

`freeze`는 랩 테이블의 `DESCRIBE FORMATTED`에서 current-snapshot-id를 읽고, 그 요청 Snapshot의 전체 고유 IMEI를 조회한다. JSON에 Snapshot 2개·목록·체크섬을 저장하며 기존 frozen.json은 덮어쓰지 않는다. SQL 문자열을 GROUP_CONCAT으로 생성하지 않고 Python에서 안전하게 구성하므로 Client의 긴 셀 내보내기 문제를 피한다.

**운영 Snapshot `518011...`, `486597...`를 랩에 사용하지 않는다.** JSON에 저장된 실제 랩 Snapshot이 이후 SQL에 자동 삽입된다. Source·요청 테이블 관리자에게 해당 Snapshot의 만료 제외를 요청하고, 시험 도중 재적재·Compaction·통계 변경은 피한다. Snapshot 고정은 데이터 내용은 고정하지만 Catalog·컬럼 통계·기동 옵션까지 고정하는 것은 아니다.

quality 로그에서 10월 11일 매칭 출력이 1,435인지 확인한다. 이 COUNT는 입력 점검이며 성능 비교용이 아니다. `config.json`의 `expected_day_rows`/`expected_full_rows`는 manifest가 다른 경우 실제 값으로 변경한다. 이후 run은 출력 행 수가 다르면 실패로 기록한다. 0행 결과로 pruning 성능을 평가하지 않는다.

## 6. 2단계: 계획과 저부하 기준 실행

```bash
python3 validate.py generate --case baseline --repeat 1
python3 validate.py plan --case baseline --repeat 1
```

생성되는 SQL:

```text
artifacts/<run_id>/baseline_r1_create.sql
artifacts/<run_id>/baseline_r1_explain.sql
artifacts/<run_id>/baseline_r1_run.sql
```

계획 확인:

- 날짜가 2026-10-11 한 개로 제한되는가?
- 양쪽 테이블의 랩 Snapshot ID가 맞는가?
- 요청 테이블이 작은 Build 쪽이며 Broadcast가 선택되는가?
- Scan·Writer hosts/instances가 랩 규모에 맞는가?
- 메모리 예약 요구가 프로세스·Pool 한도를 넘지 않는가?

확인 후:

```bash
python3 validate.py run --case baseline --repeat 1
```

각 run은 새 출력 테이블을 CREATE한 뒤 같은 impala-shell 세션에서 SET → INSERT → SUMMARY → PROFILE을 실행한다. 새 테이블은 `IF NOT EXISTS`를 사용하지 않아 기존 테이블에 조용히 추가하지 않는다. 기존 label이 있거나 실행이 실패했다면 `repeat=retry1` 등 새로운 라벨을 사용한다. 실패한 테이블은 자동 삭제하지 않는다.

결과물:

- `baseline_r1_profile.log`: 실제 INSERT의 Profile·SUMMARY.
- `baseline_r1_output_count.tsv`: 출력 수 점검. 타이밍·Profile 수집 후 별도 Query.
- `baseline_r1_manifest.json`: 옵션·Snapshot·목록 체크섬·출력 수·상태.

`client_process_seconds`는 프로세스 시작·SET·Profile 출력까지 포함한 시간이다. 보고할 Query 시간은 Profile의 Duration/Timeline이며 둘을 혼동하지 않는다.

### 캐시와 반복 순서

첫 실행은 별도 표시한다. 같은 입력을 다시 읽으면 OS 캐시·File handle cache·Metadata 캐시 영향이 있다. 처음 A만 느리고 B만 따뜻한 상태가 되지 않게 한다. 전체 시스템 Cache를 지우거나 Daemon을 재시작하지 않는다.

짧은 스크리닝 후 차이가 있으면 순서를 바꿔 반복한다. 예: A→B→C→D, 이후 D→C→B→A. 캐시 상태·동시 작업을 기록한다. 몇 초 이내 실행은 접속·코드 생성·캐시 변동 영향이 크므로 개선율을 운영에 외삽하지 않는다.

## 7. 3단계: HDFS Open·공유 경로 관측

기준 실행과 같은 시간대의 CM 시계열을 확보한다.

| 대상 | 지표·자료 | 해석 |
|---|---|---|
| NameNode | RPC Queue/Processing latency, Call queue, GC, 파일·블록 수 | Open 지연과 같은 시간대의 상승 여부 |
| Executor | Handle cache hit/miss·점유·축출, CPU·I/O | 프로세스 전체 지표와 Query별 지표 구분 |
| Profile | Open/Read/ScannerIoWait, Create timer, Local bytes | 평균 Fragment와 실제 Instance를 중복 합산하지 않음 |
| OS | iostat·vmstat·NIC | 디스크·CPU·네트워크 경합 여부 |

Web UI 포트·HTTPS·SPNEGO는 impala-shell의 TLS와 별도 설정이다. CM Role의 Web UI 링크로 실제 URL을 확인한다. 예시 `/jmx`, `/metrics?json`은 해당 서비스 설정에 맞게 사용한다.

```bash
# URL과 인증 설정을 실제 환경에 맞춘 후 별도 터미널에서 실행한다.
export NN_JMX_URL='https://ACTUAL_ACTIVE_NAMENODE:WEB_PORT/jmx'
export IMPALA_METRICS_URL='https://ACTUAL_EXECUTOR:WEB_PORT/metrics?json'
export MONITOR_OUT='monitor_baseline'
export MONITOR_SAMPLES=120
bash monitor.example.sh
```

JMX URL을 모르면 CM 차트 내보내기를 먼저 사용한다. NameNode RPC bean 이름의 포트는 실제 RPC 포트이며 Web UI 포트가 아니다. Handle cache 카운터는 누적값이므로 실행 전후 차이를 보고, 다른 Query가 없을 때만 해당 Query의 효과에 가깝게 해석한다.

Linux 노드의 읽기 전용 관측 예시:

```bash
iostat -xz 1 60 > iostat_baseline.log
vmstat 1 60 > vmstat_baseline.log
hdfs dfsadmin -report > hdfs_report.txt
```

Thread dump는 CM 진단 기능을 우선 사용한다. `jcmd -l`로 연결 가능한 JVM을 확인한 경우에만 해당 PID에 `jcmd <PID> Thread.print -l`을 사용한다. impalad는 네이티브 프로세스에 JVM이 포함된 형태이므로 Java 프로세스처럼 항상 Attach할 수 있다고 가정하지 않는다.

**통과 기준:** 랩에 Open 지연이 없다면 정상 동작을 기록하고 다음 시험으로 진행한다. 지연이 있다면 NameNode Queue인지 Client 대기인지 증거로 분리한다. NameNode Handler·캐시 용량을 먼저 바꾸지 않는다.

## 8. 4단계: 이 Query의 메모리 한도 비교

기준 설정이 35GiB이고 랩에서 승인 가능한 경우:

```bash
python3 validate.py plan --case mem8 --repeat 1
python3 validate.py run --case mem8 --repeat 1
python3 validate.py compare --left baseline:1 --right mem8:1
```

필요하고 8GiB에서 안정성이 확인된 후에만:

```bash
python3 validate.py plan --case mem6 --repeat 1
python3 validate.py run --case mem6 --repeat 1
python3 validate.py compare --left baseline:1 --right mem6:1
```

기준이 이미 8GiB면 mem8은 중복이므로 생략한다. Pool 최소·최대 Query 한도와 Clamp가 8GiB를 다른 값으로 바꾸지 않는지 Profile의 실제 옵션을 확인한다.

**성공 기준:** 37컬럼 결과와 중복이 동일하고 OOM·새 Spill이 없으며 Admission 산정 부담이 줄어든다. Duration이 같아도 자원 효율 개선으로 인정할 수 있다. 이 단계에서 동시 실행 상한을 올리지 않는다.

A/B/C/D 시험 전에는 설정 파일의 baseline_options를 공통 기준으로 확정한다. 위 메모리 비교 실행은 별도 프로세스여서 다음 세션에 SET이 남지 않지만, config를 변경했다면 변경한 공통값이 모든 Matrix에 적용되는지 확인한다.

## 9. 5단계: 정적 IN × Late materialization

### 실제 Late materialization 값이 -1일 때

| Case | 정적 IN | Late materialization |
|---|---|---:|
| A | 없음 | -1 |
| B | 있음 | -1 |
| C | 없음 | 20 |
| D | 있음 | 20 |

```bash
python3 validate.py plan --case A --repeat 1
python3 validate.py plan --case B --repeat 1
python3 validate.py plan --case C --repeat 1
python3 validate.py plan --case D --repeat 1

python3 validate.py run --case A --repeat 1
python3 validate.py run --case B --repeat 1
python3 validate.py run --case C --repeat 1
python3 validate.py run --case D --repeat 1

python3 validate.py compare --left A:1 --right B:1
python3 validate.py compare --left A:1 --right C:1
python3 validate.py compare --left A:1 --right D:1
```

기준 baseline이 A와 완전히 같은 옵션·입력이면 A 실행은 생략하고 아래처럼 기존 기준 결과를 사용한다.

```bash
python3 validate.py compare --left baseline:1 --right B:1
python3 validate.py compare --left baseline:1 --right C:1
python3 validate.py compare --left baseline:1 --right D:1
```

차이를 재확인할 필요가 있을 때 반복 순서를 역전한다. A가 기준과 같아 생략됐다면 A 반복 대신 baseline을 사용한다.

```bash
for c in D C B A; do
  python3 validate.py run --case "$c" --repeat 2
done
```

### 이미 활성화된 경우

일부러 -1로 끄지 않는다. config의 실제 기준값을 유지하고:

```bash
python3 validate.py plan --case static_current --repeat 1
python3 validate.py run --case static_current --repeat 1
python3 validate.py compare --left baseline:1 --right static_current:1
```

### 판단

- A→B: 정적 IN의 추가 효과.
- A→C: Late materialization의 추가 효과.
- B→D: IN이 있는 상태의 추가 효과.
- 읽기량이 줄지 않고 CPU만 줄면 디코딩·구체화 개선일 수 있다.
- `scattered`는 Min/Max 생략이 어렵도록 설계됐으므로 B가 A보다 크게 빠르지 않아도 설계와 일치할 수 있다.
- 결과가 틀리면 속도와 관계없이 탈락한다. `compare`는 37개 컬럼 전체를 GROUP BY하고 양쪽 중복 횟수 차이를 검사한다. NULL도 같은 그룹으로 비교한다. 해시 합계나 행 수만으로 통과시키지 않는다.

## 10. 6단계: Writer 수 제한

처음에는 원본 필터·공통 기준 옵션에서 한 요소만 바꾼다.

```bash
python3 validate.py plan --case writer8 --repeat 1
python3 validate.py plan --case writer1 --repeat 1
```

Writer Fragment의 hosts/instances가 줄면서 Source Scan 병렬성은 유지되는지 확인한다. 옵션이 SET에서 받아들여져도 Iceberg INSERT 계획에 반영되지 않으면 그 빌드에서 Writer 제한 효과가 확인된 것은 아니다.

```bash
python3 validate.py run --case writer8 --repeat 1
python3 validate.py compare --left baseline:1 --right writer8:1
# writer8의 동작을 확인한 후에만 writer1 시험
python3 validate.py run --case writer1 --repeat 1
python3 validate.py compare --left baseline:1 --right writer1:1
```

확인 지표: Writer 인스턴스, 실제 생성 파일 수, 파일 크기, Exchange·Sink 시간, 전체 Duration. Writer 8이 파일 8을 보장하지는 않는다. 랩에서는 기존 Writer 수도 적으므로 운영의 929개 파일 현상이 재현되지 않을 수 있다.

정적 IN이 선택된 최종 조건이라면 config의 `static_cases`에 `writer8`,`writer1`을 넣고 **새 repeat 라벨**로 같은 필터 기준 결과와 비교한다. Matrix의 최선 조건이 LM=20이면 공통 기준 또는 해당 case_overrides에 20을 명시하고 실제 옵션을 확인한다.

옵션이 무시되면 기존 기준 결과의 작은 출력 테이블을 Staging으로 사용한다. 새 37컬럼 출력 테이블을 만들고 아래 형태의 마지막 단계만 시험한다.

```sql
SET NUM_NODES=1;
-- SELECT *는 Staging과 최종 테이블의 37컬럼 순서가 동일할 때만 사용
INSERT INTO imei_validation.NEW_FINAL_TABLE
SELECT * FROM imei_validation.EXISTING_SMALL_RESULT_TABLE;
```

위 SQL은 테이블명 자리표시자다. 생성된 실제 테이블명으로 바꾼다. 실행 전 EXPLAIN에서 전용 Coordinator에서도 지원되는 단일 노드 계획인지 확인한다. 이 단계 비용과 원래 추출 비용을 합쳐 평가한다. 대형 Source에 NUM_NODES=1을 적용하지 않는다.

## 11. 7단계: MT_DOP와 동시 실행

```bash
python3 validate.py plan --case dop2 --repeat 1
python3 validate.py run --case dop2 --repeat 1
python3 validate.py compare --left baseline:1 --right dop2:1

python3 validate.py plan --case dop5 --repeat 1
python3 validate.py run --case dop5 --repeat 1
python3 validate.py compare --left baseline:1 --right dop5:1
```

기준이 이미 MT_DOP=5면 dop5는 중복이므로 생략한다. CPU·I/O 대기·인스턴스 수·Query 시간과 Batch 전체 처리량을 함께 본다. MT_DOP 감소가 항상 유리한 것은 아니다.

동시 실행은 위 단독 검증을 통과한 뒤 1→2→4 순서로 확대하고 새 repeat 라벨을 사용한다. 다른 시험과 겹치지 않게 한다. 3노드 랩에서 시작부터 17개 대형 Query를 실행하지 않는다.

```bash
# 두 개의 독립 출력으로 동시 실행. 사용 전 해당 case·옵션·예산을 확인한다.
python3 validate.py run --case baseline --repeat conc2a > conc2a.log 2>&1 &
p1=$!
python3 validate.py run --case baseline --repeat conc2b > conc2b.log 2>&1 &
p2=$!
wait "$p1"
wait "$p2"
```

주요 지표는 Query당 Duration·Queue와 2개 Batch 전체 완료시간이다. Admission·프로세스 메모리·NameNode RPC가 악화되면 추가 동시 실행을 늘리지 않는다. 운영 Pool 정책 변경은 이 결과를 근거로 별도 결정한다.

## 12. 8단계: 레이아웃·파일 Bloom filter·매핑 검증

이 단계는 필수 A/B를 통과한 후의 물리 설계 실험이다.

### IMEI-sorted 비교

저장소의 같은 seed·새 출력 경로로 별도 파일을 생성할 수 있다.

```bash
cd /path/to/vodafone-imei-batch/synthetic/imei_batch
python3 generate_synthetic_ipdr.py \
  --output-dir /data/imei_synth_sorted_trial01 \
  --target-gib 3 --layout imei-sorted --seed 20261011 --background
```

**기존 테이블에 `load`를 다시 실행하지 않는다.** 새 Stage·새 Iceberg Source 이름과 새 HDFS 경로를 사용하도록 적재 SQL의 복사본을 수정하거나, 고정된 기존 Source를 새로운 테이블로 정렬 재작성한다. 기존 `ipdr_db.ipdr_cisco_oct_2026`에 추가하면 데이터가 섞인다.

같은 seed라는 사실만으로 서로 다른 layout의 모든 값이 완전히 같다고 단정하지 않는다. 가장 엄밀한 비교는 동일한 고정 Source를 별도 테이블로 재배치하는 것이다. 새 Source의 행·값·요청 매칭 결과를 검증하고, 새 run_id·새 config로 freeze한다. 최종 Iceberg 파일 Min/Max와 row group 경계를 확인한 뒤 정적 IN 유무를 비교한다.

### 파일 Bloom filter

새 별도 Source에만 적용하며 기존 파일은 자동 갱신되지 않는다. Impala Writer의 설정 예시는 다음과 같다. Iceberg 대상에서 해당 빌드가 이 테이블 속성을 실제 사용하는지 검증한다.

```sql
ALTER TABLE lab_layout.NEW_SOURCE SET TBLPROPERTIES (
  'parquet.bloom.filter.columns'='imei_14:1048576'
);
SET PARQUET_BLOOM_FILTER_WRITE=IF_NO_DICT;
-- 새 데이터를 이 별도 Source에 쓰고, 고정 Snapshot으로 읽기 시험
```

필터 크기는 시험값이다. 입력 Writer가 PyArrow/Spark이면 그 Writer의 속성·옵션을 사용해야 하며 위 Impala 설정이 자동 적용되지 않는다. Parquet Footer에서 필터 존재 여부를 확인하고 `NumBloomFilteredRowGroups`, BytesRead, 파일 크기·쓰기시간을 비교한다. Runtime Bloom filter와 파일 Bloom filter는 다른 기능이다.

### IMEI→MSISDN 매핑

합성데이터는 IMEI와 MSISDN이 1:1이므로 유리한 결과가 나와도 운영에 외삽하지 않는다. 같은 날짜에 다중 MSISDN, NULL, 변경·재할당·지연 도착을 별도 케이스로 검증한다. 원본 IMEI Join을 유지하고 잘못된 매핑으로 결과를 제거하지 않도록 한다.

## 13. 전체 7일 최종 검증

최선의 옵션만 `config.json`의 `final_candidate_options`에 넣는다. 예시는 실측으로 선택됐을 때만 사용한다.

```json
"final_candidate_options": {
  "MEM_LIMIT": "8g",
  "PARQUET_LATE_MATERIALIZATION_THRESHOLD": "20",
  "MAX_FS_WRITERS": "8"
}
```

`final_candidate_static`이 true이면 정적 IN을 포함한다. 정적 IN이 채택되지 않았다면 config에서 `"final_candidate_static": false`로 바꾼다. 선택하지 않은 개선을 억지로 묶지 않는다.

```bash
python3 validate.py plan --case final_base --repeat 1
python3 validate.py plan --case final_candidate --repeat 1
python3 validate.py run --case final_base --repeat 1
python3 validate.py run --case final_candidate --repeat 1
python3 validate.py compare --left final_base:1 --right final_candidate:1
```

같은 랩 Snapshot의 완전한 7일 기준 출력이 이미 있다면 기준 재실행을 피할 수 있다. 이 helper의 compare는 기록된 두 실행을 비교하므로 외부 결과를 재사용하려면 SQL의 비교 대상을 검토해 별도 대조한다. 기존 Profile의 행 수만으로는 동일성을 입증할 수 없다.

정상 입력이면 두 결과는 8,607행이어야 한다. 행 수 일치와 37컬럼 multiset equality를 모두 통과한 후 Duration·읽기량·CPU·파일 수를 보고한다.

## 14. 결과 요약과 판정

```bash
python3 validate.py summarize
python3 profile_fields.py artifacts/<run_id>/*_profile.log
```

`summary.csv`는 상태·Client 프로세스 시간·Profile Duration·전체 BytesRead·CPU·Admission 값을 모은다. 평균 Open·Wait, Writer 파일 수 등은 `.fields.txt`와 원본 Profile에서 같은 범위를 선택한다. 전체·평균·개별 Instance를 함께 합산하지 않는다. 원본 Profile을 보존해 누락된 자동 추출 필드를 확인한다.

| 비교 | 정합성 | 성능·자원 판정 |
|---|---|---|
| baseline vs mem8 | 37컬럼·중복 동일 | Admission 감소, OOM·Spill 없음 |
| A vs B | 동일 | 읽기·skip·Duration의 정적 IN 효과 |
| A vs C, B vs D | 동일 | CPU·구체화·Duration의 LM 효과 |
| baseline vs writer8 | 동일 | Writer·파일 감소, Scan 병렬성 유지 |
| baseline vs dop2 | 동일 | 단일 Query와 전체 처리량 균형 |
| final_base vs final_candidate | 8,607행 및 전체값 동일 | 7일 완료시간·비용 종합 평가 |

고정된 퍼센트 통과 기준을 미리 만들지 않는다. 예를 들어 CPU만 줄고 I/O 대기가 같다면 CPU 개선으로 보고하고 전체시간 단축으로 포장하지 않는다. 미세한 차이는 순서 교차 반복 후 중앙값과 변동 폭을 제시한다. 클러스터 노드 수, 실제 스캔 파일·바이트·row group·실행 옵션을 반드시 함께 기록한다.

## 15. 선택 사항: 정합성 경계 사례 검증

SDV 데이터에 NULL·경계·중복이 부족할 수 있다. 다음은 Source·요청을 건드리지 않는 별도 fixture다.

```bash
python3 make_semantic_fixture.py
# 생성된 semantic_fixture.sql을 먼저 검토한다.
impala-shell -i ccycloud-2.sbimf-azure.root.comops.site -d default \
  -k --ssl \
  --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
  -f semantic_fixture.sql > semantic_fixture.log 2>&1

python3 validate.py --config config.fixture.json freeze
python3 validate.py --config config.fixture.json run --case baseline --repeat 1
python3 validate.py --config config.fixture.json run --case static_current --repeat 1
python3 validate.py --config config.fixture.json compare --left baseline:1 --right static_current:1
```

fixture는 예상 출력 12행이다. 시작·종료 NULL, BETWEEN 경계, 동일 Source 중복, 서로 다른 req_id, 세션이 요청 전체를 감싸는 경우, Partition 날짜 밖의 요청 기간과 일치하는 종료시각, 선행 0을 포함한다. `cross_date`가 1행 남아야 하며, 기간이 겹친다는 이유만으로 OR를 일반 overlap 조건으로 바꾸거나 날짜로 요청을 제한하면 결과가 달라진다는 것을 확인한다.

## 16. 공식 근거

- [검토한 저장소 README](https://github.com/jshin-jackson/vodafone-imei-batch/blob/49213c845d977453cd829fc04d43e3477b61d149/README.md)
- [생성기 구현](https://github.com/jshin-jackson/vodafone-imei-batch/blob/49213c845d977453cd829fc04d43e3477b61d149/synthetic/imei_batch/generate_synthetic_ipdr.py)
- [적재 구현: Iceberg INSERT 재작성](https://github.com/jshin-jackson/vodafone-imei-batch/blob/49213c845d977453cd829fc04d43e3477b61d149/synthetic/imei_batch/sql/02_load_parquet.sql)
- [Impala-shell 옵션·Profile 출력](https://impala.apache.org/docs/build/plain-html/topics/impala_shell_options.html)
- [Impala-shell 명령](https://impala.apache.org/docs/build/html/topics/impala_shell_commands.html)
- [Iceberg Snapshot 조회](https://impala.apache.org/docs/build/html/topics/impala_iceberg.html)
- [Cloudera 성능·파일 Bloom filter](https://docs.cloudera.com/runtime/7.3.1/impala-reference/impala-reference.pdf)
- [NameNode RPC JMX 지표](https://cwiki.apache.org/confluence/spaces/EAG/pages/67634265/Hadoop%2BJMX%2BMonitoring%2Band%2BAlerting)
