"""
전일 대비 가격 변동 분석
두 날짜의 수집 데이터를 비교해서 품목별 변동률 산출

사용: python compare.py --today 2026-03-09 --prev 2026-03-06
"""

import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

DATA_DIR = Path(__file__).parent / "data"
REPORT_DIR = Path(__file__).parent / "reports"

from data_loader import load_data, ARCHIVE_DIR


def aggregate_by_product(data: dict) -> dict[str, dict]:
    """전체 데이터를 품목별로 집계 (kg당 단가 기준)"""
    stats: dict[str, dict] = {}
    for market in data["markets"].values():
        for item in market["items"]:
            product = item["product"]
            unit_wt = item.get("unit_weight", 0)
            price = item.get("price", 0)
            per_kg = price / unit_wt if unit_wt > 0 else 0
            if per_kg <= 0:
                continue
            if product not in stats:
                stats[product] = {"per_kg_prices": [], "count": 0, "total_qty": 0}
            stats[product]["per_kg_prices"].append(per_kg)
            stats[product]["count"] += 1
            qty = item["quantity"]
            if isinstance(qty, (int, float)):
                stats[product]["total_qty"] += qty
    return stats


def _filter_outliers(prices: list) -> list:
    """이상치 제거 — 절대 상한 + 중앙값 기준 5배 초과 or 1/5 미만 제외"""
    prices = [p for p in prices if p <= 100_000]
    if len(prices) < 3:
        return prices
    sorted_p = sorted(prices)
    median = sorted_p[len(sorted_p) // 2]
    if median == 0:
        return prices
    return [p for p in prices if median / 5 <= p <= median * 5]


def compare(today_date: str, prev_date: str) -> str:
    """두 날짜 비교"""
    today = load_data(today_date)
    prev = load_data(prev_date)

    if not today:
        return f"오늘({today_date}) 데이터가 없습니다."
    if not prev:
        return f"전일({prev_date}) 데이터가 없습니다."

    today_stats = aggregate_by_product(today)
    prev_stats = aggregate_by_product(prev)

    lines = [
        f"# 전일 대비 가격 변동 ({prev_date} → {today_date})\n",
        f"| 품목 | 전일 (원/kg) | 오늘 (원/kg) | 변동률 | 거래건수 |",
        f"|------|------------|------------|--------|---------|",
    ]

    # 오늘 거래건수 상위 품목 기준
    sorted_products = sorted(
        today_stats.items(),
        key=lambda x: x[1]["count"],
        reverse=True,
    )

    up_count = 0
    down_count = 0
    stable_count = 0

    for product, t_stat in sorted_products[:30]:
        if product not in prev_stats:
            continue
        p_stat = prev_stats[product]

        t_prices = _filter_outliers(t_stat["per_kg_prices"])
        p_prices = _filter_outliers(p_stat["per_kg_prices"])
        if not t_prices or not p_prices:
            continue
        t_avg = sum(t_prices) / len(t_prices)
        p_avg = sum(p_prices) / len(p_prices)

        if p_avg == 0:
            continue

        change = ((t_avg - p_avg) / p_avg) * 100

        if change > 1:
            arrow = "🔺"
            up_count += 1
        elif change < -1:
            arrow = "🔻"
            down_count += 1
        else:
            arrow = "➖"
            stable_count += 1

        lines.append(
            f"| {product} | {p_avg:,.0f} | {t_avg:,.0f} | "
            f"{arrow} {change:+.1f}% | {t_stat['count']:,}건 |"
        )

    lines.append(f"\n**요약**: 상승 {up_count}개, 하락 {down_count}개, 보합 {stable_count}개")
    lines.append(f"(거래건수 상위 30개 품목, 변동률 ±1% 이내 = 보합)")

    result = "\n".join(lines)

    # 저장
    REPORT_DIR.mkdir(exist_ok=True)
    out = REPORT_DIR / f"compare_{prev_date}_vs_{today_date}.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(result)
    print(f"비교 리포트 저장: {out}")

    return result


def find_prev_date(today_str: str) -> str | None:
    """오늘 기준 가장 최근 이전 데이터 파일 찾기 (data/ + 아카이브)"""
    today = datetime.strptime(today_str, "%Y-%m-%d")
    for i in range(1, 8):
        prev = today - timedelta(days=i)
        prev_str = prev.strftime("%Y-%m-%d")
        # data/ 폴더
        if (DATA_DIR / f"auction_{prev_str}.json").exists():
            return prev_str
        # 아카이브
        month = prev_str[:7]
        if (ARCHIVE_DIR / month / f"auction_{prev_str}.json").exists():
            return prev_str
    return None


def main():
    parser = argparse.ArgumentParser(description="전일 대비 가격 변동 분석")
    parser.add_argument("--today", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--prev", default="", help="비교 날짜 (빈값이면 자동 탐색)")
    args = parser.parse_args()

    prev = args.prev or find_prev_date(args.today)
    if not prev:
        print("비교할 이전 날짜 데이터가 없습니다.")
        sys.exit(1)

    print(f"비교: {prev} → {args.today}")
    result = compare(args.today, prev)
    print("\n" + result)


if __name__ == "__main__":
    main()
