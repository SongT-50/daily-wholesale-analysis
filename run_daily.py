"""
도매시장 일일 분석 통합 실행기
수집 → 출하예약 수집 → 분석 → 전일대비 비교 → 리포트 저장

GitHub Actions에서 호출하거나, 수동 실행 가능
사용: python run_daily.py [--date 2026-03-09]
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from collect import collect, backfill, DEFAULT_MARKETS
from collect_shipment import collect_shipment
from analyze import generate_report
from compare import compare, find_prev_date


def main():
    parser = argparse.ArgumentParser(description="도매시장 일일 분석 파이프라인")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="분석 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    date = args.date
    tomorrow = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"{'='*60}")
    print(f"  도매시장 일일 분석 파이프라인")
    print(f"  날짜: {date}")
    print(f"{'='*60}\n")

    # Step 0: 공판장 정산 지연 보정 (D-1, D-2 재수집)
    # 공판장(농협/원협)은 청과법인보다 정산 업로드가 1~2일 늦음
    print("[0/5] 공판장 정산 보정 (D-1, D-2)")
    print("-" * 40)
    backfill_dates = []
    for delta in [1, 2]:
        bf_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=delta)).strftime("%Y-%m-%d")
        backfill_dates.append(bf_date)
    backfilled = []
    for bf_date in backfill_dates:
        updated = backfill(bf_date, DEFAULT_MARKETS)
        if updated:
            backfilled.append(bf_date)

    # Step 1: 정산 데이터 수집
    print(f"\n[1/5] 정산 데이터 수집")
    print("-" * 40)
    data = collect(date, DEFAULT_MARKETS)

    total = data.get("total_collected", data.get("total_count", 0))
    if total == 0:
        print("\n수집된 데이터가 없습니다. (휴장일이거나 API 오류)")
        sys.exit(0)

    # Step 2: 전자송품장 출하예약 수집 (내일 출하예정 물량)
    print(f"\n[2/5] 전자송품장 출하예약 수집 ({tomorrow})")
    print("-" * 40)
    shipment = collect_shipment(tomorrow, DEFAULT_MARKETS)
    shipment_total = shipment.get("total_collected", 0)

    # Step 3: AI 분석 (출하예약 데이터 포함)
    print(f"\n[3/5] AI 분석 리포트 생성")
    print("-" * 40)
    report = generate_report(date, shipment_date=tomorrow)

    # Step 4: 전일 대비 비교
    print(f"\n[4/5] 전일 대비 가격 변동")
    print("-" * 40)
    prev_date = find_prev_date(date)
    if prev_date:
        print(f"비교 대상: {prev_date}")
        compare(date, prev_date)
    else:
        print("비교할 이전 데이터가 없습니다. (첫 수집)")

    # Step 5: 보정된 날짜 대시보드 요약 재생성
    if backfilled:
        print(f"\n[5/5] 보정 데이터 대시보드 요약 재생성")
        print("-" * 40)
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent / "wholesale-dashboard"))
            from preprocess import preprocess
            for bf_date in backfilled:
                preprocess(bf_date)
        except Exception as e:
            print(f"  대시보드 요약 재생성 실패: {e}")
    else:
        print(f"\n[5/5] 보정 변동 없음 — 대시보드 재생성 건너뜀")

    # 결과
    print(f"\n{'='*60}")
    print("  완료!")
    if backfilled:
        print(f"  보정: {', '.join(backfilled)} (공판장 정산 반영)")
    print(f"  정산: data/auction_{date}.json ({total:,}건)")
    print(f"  출하예약: data/shipment_{tomorrow}.json ({shipment_total:,}건)")
    print(f"  리포트: reports/report_{date}.md")
    if prev_date:
        print(f"  비교: reports/compare_{prev_date}_vs_{date}.md")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
