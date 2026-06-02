"""정산 보고서 월 경계 / LAG 컷 로직 단위테스트.

2026-06-02 버그 회귀 방지:
  달이 막 바뀐 직후(6/1~6/3), 자동 메일이 '어제 기준 월'(6월)로 잡아
  미완성인 새 달 첫날(6/1, 공판장 미정산)을 보고서 마지막 정산일로 발송하던 버그.
  → 기준 월을 '오늘-LAG'(안정화 기준일)의 달로 잡아, 새 달 데이터가 익기 전엔
    전월 누계를 유지하고 D+LAG 지나면 자동 전환하도록 수정.

실행: python test_settlement_range.py   (의존: pytest 불필요, 순수 assert)
"""
import sys
from datetime import date
import settlement_report as sr

sys.stdout.reconfigure(encoding="utf-8")


def make_load_day(settled):
    """settled: dict[date, int] → 그 날 정산 완료된 법인 수(present 개수).
    지정 안 된 날은 0(미수집). load_day는 (records, present) 반환."""
    def fake(d):
        n = settled.get(d, 0)
        return ([{}] if n else []), list(range(n))
    return fake


def all_settled():
    """모든 날 4법인 완비 (start 결정 로직만 보고 싶을 때)."""
    return lambda d: ([{}], [0, 1, 2, 3])


PASS, FAIL = 0, 0

def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


def test_month_boundary_start():
    """기준 월(start) = (오늘-LAG)의 달. 월/연 경계."""
    print("[1] 월/연 경계 기준월 결정")
    sr.load_day = all_settled()
    cases = [
        (date(2026, 6, 2),  date(2026, 5, 1),  "6/2 → 5월 유지(새 달 미완)"),
        (date(2026, 6, 3),  date(2026, 5, 1),  "6/3 → 5월 유지"),
        (date(2026, 6, 4),  date(2026, 6, 1),  "6/4 → 6월 전환(6/1이 D-3)"),
        (date(2026, 5, 31), date(2026, 5, 1),  "5/31 → 5월"),
        (date(2026, 5, 15), date(2026, 5, 1),  "5/15 → 5월"),
        (date(2026, 1, 2),  date(2025, 12, 1), "1/2 → 전년 12월(연 경계)"),
        (date(2026, 3, 3),  date(2026, 2, 1),  "3/3 → 2월(전월 유지)"),
    ]
    for today, exp_start, label in cases:
        start, _ = sr.resolve_report_range(today)
        check(label, start == exp_start, f"got start={start}")


def test_no_premature_new_month():
    """★핵심 회귀: 6/2에 6/1이 미완비여도 절대 6/1을 잡지 않는다."""
    print("[2] 6/2 버그 회귀 방지 (미완비 6/1 미발송)")
    settled = {date(2026, 5, d): 4 for d in range(1, 32)}  # 5월 전부 완비
    settled[date(2026, 6, 1)] = 2  # 공판장 2곳(원협노은·농협대전) 미정산
    sr.load_day = make_load_day(settled)
    start, end = sr.resolve_report_range(date(2026, 6, 2))
    check("start = 5/1 (6월 아님)", start == date(2026, 5, 1), f"got {start}")
    check("end ≤ 5/30 (LAG 컷 준수)", end <= date(2026, 5, 30), f"got {end}")
    check("end ≠ 6/1 (미완비 미발송)", end != date(2026, 6, 1), f"got {end}")


def test_lag_backfill():
    """LAG 컷 + 백필 미완: cutoff(5/30)·5/29 미완비면 5/28로 후퇴."""
    print("[3] LAG 컷 + 백필 미완 후퇴")
    settled = {date(2026, 5, d): 4 for d in range(1, 29)}  # ~5/28 완비
    settled[date(2026, 5, 29)] = 3
    settled[date(2026, 5, 30)] = 2
    sr.load_day = make_load_day(settled)
    start, end = sr.resolve_report_range(date(2026, 6, 2))
    check("start = 5/1", start == date(2026, 5, 1), f"got {start}")
    check("end = 5/28 (4법인 완비 마지막)", end == date(2026, 5, 28), f"got {end}")


def test_new_month_transition():
    """6/4: 6/1이 D-3 지나 완비 → 6월 누계로 자연 전환."""
    print("[4] 새 달 전환 (6/4 → 6월)")
    settled = {date(2026, 5, d): 4 for d in range(1, 32)}
    settled[date(2026, 6, 1)] = 4  # D-3 지나 백필 완료
    sr.load_day = make_load_day(settled)
    start, end = sr.resolve_report_range(date(2026, 6, 4))
    check("start = 6/1 (6월 전환)", start == date(2026, 6, 1), f"got {start}")
    check("end = 6/1", end == date(2026, 6, 1), f"got {end}")


def test_mid_month_normal():
    """월 중순 평상시: cutoff까지 완비면 cutoff가 마지막 정산일."""
    print("[5] 월 중순 평상시")
    settled = {date(2026, 5, d): 4 for d in range(1, 32)}
    sr.load_day = make_load_day(settled)
    start, end = sr.resolve_report_range(date(2026, 5, 20))
    check("start = 5/1", start == date(2026, 5, 1), f"got {start}")
    check("end = 5/17 (today-LAG)", end == date(2026, 5, 17), f"got {end}")


if __name__ == "__main__":
    _orig = sr.load_day  # 복원용
    try:
        test_month_boundary_start()
        test_no_premature_new_month()
        test_lag_backfill()
        test_new_month_transition()
        test_mid_month_normal()
    finally:
        sr.load_day = _orig
    print(f"\n{'='*50}\n결과: {PASS} PASS / {FAIL} FAIL")
    sys.exit(1 if FAIL else 0)
