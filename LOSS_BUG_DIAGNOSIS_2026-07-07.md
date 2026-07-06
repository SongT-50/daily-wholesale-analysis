# 일일 collect 데이터 소실버그 근본 진단 (2026-07-07 WHOLESALE-T3)

> 트리거: 태은이 catch(7/6) — "하루씩 단일법인 데이터 소실"이 5/11 농협대전 이후 반복
> (2/23 원협·2/24 중앙·6/22 중앙·6/30 원협). "근본 점검 필요"로 남긴 🔴 trigger.
> 결론: **3중 결함이 겹친 소실**. 근본 방어 = collect/backfill에 시장별 union merge 이식.

## 증상 (반복 패턴)
- 특정 날짜에 **한 법인(시장)이 통째로 0건**으로 남는 반쪽 파일.
- 예: 6/30 `auction_2026-06-30.json` = 21,593건(원협 0) ↔ 실제 완전본 42,701건.
- 6/22(중앙 0)·2/23(원협 0)·2/24(중앙 0, aT에도 없어 복구불가)·5/11(농협대전).
- 복구는 매번 `recollect_month.py`(시장별 안전머지)로 사후 수동 처리해 왔음.

## 코드 경로 (확정)
- workflow `daily-analysis.yml` line 33-47: `BASE=어제`, `for i in 0 1 2 3`
  → `data/auction_${DATE}.json`이 **없으면** `run_daily.py --date $DATE`.
- `run_daily.py`: D-1~D-5 `backfill()` + 당일 `collect()`.
- 경로 상수: `OUTPUT_DIR=data/`(repo, git 추적·GA commit), `ARCHIVE_DIR=../../wholesale-data`
  (**repo 밖 = GA 러너엔 없음, 로컬 전용**). → **GA에서 유효한 저장소는 `data/` 하나뿐.**

## 3중 결함

### 결함 1 — `Clean old data` step이 전월 완전본을 삭제 (트리거)
- workflow line 195-217: `data/` 안에서 **당월 + 수집월(어제의 월) 외 auction_*.json 전부 `rm`**.
- 월초(예 7/2, 어제=7/1이라 수집월=07): **6월 완전본 전부 삭제 대상**.
- ※ STATE 7/2 기록대로 commit step의 `git add data/auction_*.json`(셸 glob=존재파일만)이
  삭제를 스테이징 못 해 origin엔 "우연히" 남는 구조 → **불안정한 보호**(의존 금지).

### 결함 2 — `collect()`에 손실방지 전혀 없음 (반쪽 저장)
- `collect()` line 310-320: 새 수집 결과를 `data/` + 아카이브에 **무조건 덮어쓰기**.
- 완전본이 삭제된 뒤 그 날짜를 재수집하면(위 `for i` loop), 그날 aT API가
  특정 시장 정산분을 롤백/미제공하면 그 시장 0건 반쪽으로 새로 씀 → 소실.

### 결함 3 — `backfill()`이 전체 총건수만 비교 (시장 단위 못 잡음)
- `backfill()` line 363-375: `new_count > old_count`면 교체, 아니면 원본 복원.
- **전체 total_collected 비교뿐** → ⓐ한 시장 통째 빠짐이 다른 시장 증가로 상쇄되면 통과,
  ⓑD+5 안정화 skip(line 353)에 걸리면 재수집 자체를 안 함 → 반쪽 고착.
- 대조: `recollect_month.recollect_date`(line 66-77)는 **시장별로** `new>=old` 비교 →
  한 시장이 0건이어도 기존 시장 보존. **이 로직이 정답인데 일일 경로엔 없음.**

## 소실 메커니즘 재구성 (가장 유력 — 로그 부재로 100% 단정 X, W-13)
1. 6/30 완전본(42,701)이 data/에 매일 commit.
2. 어느 시점 6/30 파일이 data/에서 사라짐(월초 clean, 또는 재실행 타이밍).
3. `for i` loop이 6/30 없음 감지 → `run_daily --date 6/30` → `collect(6/30)`.
4. 그 시점 aT가 6/30 원협(250003) 정산분 미제공 → 원협 0건 반쪽 저장(무손실방지).
5. 6/30이 D+5 경과 → backfill 재수집 skip → **반쪽 고착** → origin 커밋 → 발송본과 불일치.

## 수정안 (근본 방어 = 시장별 union merge)

### 핵심: `collect()` 저장 직전 + `backfill()`을 시장별 병합으로
`recollect_date`와 동일 원칙의 helper 신설:
```python
def _merge_markets_preserve(old_result: dict, new_result: dict) -> dict:
    """시장별 안전머지 — 각 시장 items 많은 쪽 보존. 반쪽이 완전본을 못 덮게."""
    if not old_result:
        return new_result
    old_m = old_result.get("markets", {})
    new_m = new_result.get("markets", {})
    merged = {}
    for code in set(old_m) | set(new_m):
        o, n = old_m.get(code, {}), new_m.get(code, {})
        merged[code] = n if len(n.get("items", [])) >= len(o.get("items", [])) else o
    r = dict(new_result)
    r["markets"] = merged
    r["total_collected"] = sum(len(m.get("items", [])) for m in merged.values())
    r["total_available"] = sum(m.get("total_available", 0) for m in merged.values())
    return r
```
- `collect()`: `data/`(및 아카이브) 기존 파일 있으면 저장 전 `_merge_markets_preserve(old, result)`.
- `backfill()`: line 363-375의 total 비교를 helper 병합으로 교체(항상 시장별 보존).

### 보조: `Clean old data` step 완화 (선택)
- 전월 완전본 삭제가 트리거이므로, 최소 **직전 월 1개는 유지**하거나
  clean 대상에서 `auction_*` 제외(용량 여유 시). 단 union merge가 있으면 소실은
  clean과 무관하게 막히므로 clean은 유지해도 안전(우선순위 낮음).

## 리스크 / 엣지케이스
- union merge의 유일한 부작용: **정당한 감소**(이상치 로직 강화로 재수집이 줄어야 할 때
  옛 데이터 유지). 정산 데이터는 확정분 append 성격이라 "많은 쪽=옳음"이 안전.
  실제로 복구에 이미 이 방식(recollect_date)을 써왔음 = 검증된 안전.

## 테스트 계획 (결재 후)
1. 인위적 반쪽 fixture(원협 items 제거) 만들어 `_merge_markets_preserve`가 완전본 보존 확인.
2. 이미 복구된 6/22·6/30 완전본에 backfill 재수집 시뮬 → 건수 불변(감소 안 함) 확인.
3. `collect()` 정상 당일 수집(기존 파일 없음) → 병합이 no-op인지 확인.

## 결재 요청 (🔴 태은이)
- 위 union merge fix를 `collect.py`에 적용 + 로컬 테스트 후 push (daily-wholesale-analysis).
- 본업 데이터 소스(매일 정산메일) 변경이라 결재 대기 — 진단·설계는 완료, 편집·push만 대기.
