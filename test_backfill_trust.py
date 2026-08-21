# -*- coding: utf-8 -*-
"""backfill() 안정화 skip 판정 대조군 (2026-08-21 WHOLESALE-T3)

무엇을 지키나:
  예전 skip 조건은 수집 '시각' 만 봤다. 건수도 실패도 안 봤다.
  그래서 조회가 통째로 실패해 0건이 저장돼도 D+5 가 지나면 '안정화됨' 으로
  분류되어 영영 다시 받지 않았다.
  실물 = 2026-08-18. 원천에 38,503건이 있는데 우리 파일은 0건이었고,
        8/23 이면 이 skip 에 걸려 굳을 참이었다.

시험 방법:
  실제 API 를 부르지 않는다. collect() 를 가짜로 바꿔치기하고
  '재수집을 시도했는가' 만 본다. 그래야 대조군이 싸고 반복 가능하다.

반증자:
  BT2 가 이 시험의 반증자다 — 일요일처럼 진짜 0건인 날까지 매번 재수집하면
  이 처방은 과잉이다. BT2 가 실패하면 조건이 너무 넓은 것이다.
  BT4 는 회귀 게이트 — 정상 파일이 종전대로 skip 되는지.
"""
import json
import sys
import tempfile
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent))

import collect as C  # noqa: E402

CALLS = []


def fake_collect(date, market_codes):
    CALLS.append(date)
    return {"total_collected": 99999}


def run_case(name, filedata, date, expect_recollect):
    """filedata=None 이면 파일 자체가 없는 경우."""
    CALLS.clear()
    tmp = Path(tempfile.mkdtemp())
    old_out, old_collect = C.OUTPUT_DIR, C.collect
    C.OUTPUT_DIR = tmp
    C.collect = fake_collect
    try:
        if filedata is not None:
            (tmp / f"auction_{date}.json").write_text(
                json.dumps(filedata, ensure_ascii=False), encoding="utf-8")
        C.backfill(date, {"250003": "대전노은"})
        did = date in CALLS
    finally:
        C.OUTPUT_DIR, C.collect = old_out, old_collect
    ok = (did == expect_recollect)
    print("  %-4s %-58s %s" % (
        "OK" if ok else "FAIL", name,
        "재수집함" if did else "skip함"))
    return ok


D = "2026-08-01"          # 대상일
LATE = "2026-08-10"       # 수집일 = D+9 (D+5 훨씬 지남)

print("=" * 74)
print("backfill 안정화 skip 판정")
print("=" * 74)
results = []

results.append(run_case(
    "BT1 구버전·0건·D+9  → 판단 불가라 다시 받아야 한다 (★실물 8/18)",
    {"total_collected": 0, "collected_at": LATE + "T00:00:00"},
    D, expect_recollect=True))

results.append(run_case(
    "BT2 신버전·실패0·0건·D+9 → 진짜 0건(일요일)이라 skip [반증자]",
    {"total_collected": 0, "collected_at": LATE + "T00:00:00", "fetch_errors": 0},
    D, expect_recollect=False))

results.append(run_case(
    "BT3 신버전·실패32·0건·D+9 → 조회 실패라 다시 받아야 한다",
    {"total_collected": 0, "collected_at": LATE + "T00:00:00", "fetch_errors": 32},
    D, expect_recollect=True))

results.append(run_case(
    "BT4 구버전·35550건·D+9 → 종전대로 skip [회귀 게이트]",
    {"total_collected": 35550, "collected_at": LATE + "T00:00:00"},
    D, expect_recollect=False))

results.append(run_case(
    "BT5 신버전·실패0·35550건·D+9 → skip",
    {"total_collected": 35550, "collected_at": LATE + "T00:00:00", "fetch_errors": 0},
    D, expect_recollect=False))

results.append(run_case(
    "BT6 D+1 (아직 안 익음) → 항상 재수집",
    {"total_collected": 100, "collected_at": "2026-08-02T00:00:00", "fetch_errors": 0},
    D, expect_recollect=True))

results.append(run_case(
    "BT7 파일 없음 → 재수집",
    None, D, expect_recollect=True))

print("=" * 74)
print("%d/%d 통과" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
