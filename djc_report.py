"""
대전중앙청과㈜ 경영 분석 리포트
우리 법인 성과 + 대전 지역 경쟁 비교 + 전국 포지션
"""
import json
import sys
import re
import os
import smtplib
import argparse
from collections import defaultdict, Counter
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv(Path(__file__).parent.parent / ".env")

DATA_DIR = Path(__file__).parent / "data"
REPORT_DIR = Path(__file__).parent / "reports"

OUR_CORP = "대전중앙청과"  # 포함 매칭
OUR_MARKET = "대전노은"

DAEJEON_MARKETS = ["대전노은", "대전오정"]

WEEKDAYS = ["월", "화", "수", "목", "금", "토", "일"]


from data_loader import load_data


def _filter_outliers(prices: list) -> list:
    prices = [p for p in prices if p <= 100_000]
    if len(prices) < 3:
        return prices
    sorted_p = sorted(prices)
    median = sorted_p[len(sorted_p) // 2]
    if median == 0:
        return prices
    return [p for p in prices if median / 5 <= p <= median * 5]


def _generate_fallback_report(corp_data, date, weekday):
    """대전중앙청과 데이터가 없을 때 대전 다른 법인 현황 리포트"""
    lines = []
    lines.append("# 대전중앙청과㈜ 경영 분석 리포트")
    lines.append(f"**{date} ({weekday}) 정산 기준**\n")
    lines.append("---")
    lines.append("## ⚠️ 대전중앙청과㈜ 경매 데이터 없음\n")
    lines.append("이 날짜에 대전중앙청과㈜의 경매/정산 데이터가 API에 없습니다.")
    lines.append("(휴장, 정산 미완료, 또는 API 지연 가능)\n")

    # 대전 다른 법인 현황
    daejeon_corps = {
        k: v for k, v in corp_data.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }

    if daejeon_corps:
        total_dj = sum(v["count"] for v in daejeon_corps.values())
        dj_sorted = sorted(daejeon_corps.items(), key=lambda x: x[1]["count"], reverse=True)

        lines.append("---")
        lines.append("## 대전 다른 법인 현황 (참고)\n")
        lines.append("| 순위 | 시장 | 법인 | 거래건수 | 물량(톤) | 금액(만원) | 품목수 |")
        lines.append("|------|------|------|---------|---------|----------|--------|")
        for i, (k, v) in enumerate(dj_sorted, 1):
            market, corp = k.split("|")
            lines.append(f"| {i} | {market} | {corp} | {v['count']:,} | {v['total_kg']/1000:,.1f} | {v['amount']/10000:,.0f} | {len(v['products'])} |")
        lines.append(f"\n대전 전체 거래: **{total_dj:,}건** (대전중앙청과 제외)")

    # 전국 요약
    total_national = sum(v["count"] for v in corp_data.values())
    lines.append(f"\n전국 전체: **{total_national:,}건** / {len(corp_data)}개 법인")
    lines.append(f"\n*자동 생성 by 송봇 | data.go.kr 정산 데이터 기준*")

    return "\n".join(lines)


def _find_complete_date(date: str, max_lookback: int = 3) -> tuple[str, dict]:
    """공판장(농협/원협) 정산이 포함된 가장 최근 데이터를 찾는다.

    공판장은 청과법인보다 정산 업로드가 1~2일 늦으므로,
    당일 데이터에 공판장이 없으면 D-1, D-2, D-3까지 탐색.
    반환: (날짜, 데이터)
    """
    from datetime import timedelta

    GONGPAN_KEYWORDS = ["(공)", "농협", "원협"]

    for delta in range(0, max_lookback + 1):
        check_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=delta)).strftime("%Y-%m-%d")
        d = load_data(check_date)
        if not d:
            continue
        # 공판장 법인이 있는지 확인
        has_gongpan = False
        for m in d["markets"].values():
            for item in m["items"]:
                corp = item.get("corp_name", "")
                if any(kw in corp for kw in GONGPAN_KEYWORDS):
                    has_gongpan = True
                    break
            if has_gongpan:
                break
        if has_gongpan:
            return check_date, d

    # 공판장 없어도 당일 데이터라도 반환
    d = load_data(date)
    return date, d


def _aggregate_data(data: dict) -> dict:
    """하루치 데이터를 법인별로 집계하여 반환."""
    corp_data = defaultdict(lambda: {
        "count": 0, "total_kg": 0, "amount": 0, "products": defaultdict(lambda: {
            "count": 0, "prices": [], "total_kg": 0, "amount": 0, "origins": Counter()
        })
    })
    for m in data["markets"].values():
        for item in m["items"]:
            market = item["market_name"]
            corp = item["corp_name"]
            key = f"{market}|{corp}"
            product = item["product"]
            unit_wt = item.get("unit_weight", 0)
            price = item.get("price", 0)
            qty = item["quantity"] if isinstance(item["quantity"], (int, float)) else 0
            amount = price * qty
            kg = unit_wt * qty
            per_kg = amount / kg if kg > 0 else 0

            corp_data[key]["count"] += 1
            corp_data[key]["total_kg"] += kg
            corp_data[key]["amount"] += amount
            corp_data[key]["products"][product]["count"] += 1
            corp_data[key]["products"][product]["total_kg"] += kg
            corp_data[key]["products"][product]["amount"] += amount
            if per_kg > 0:
                corp_data[key]["products"][product]["prices"].append(per_kg)
            origin = item.get("origin", "-")
            if origin and origin != "-":
                corp_data[key]["products"][product]["origins"][origin] += 1
    return corp_data


def _aggregate_monthly(date: str) -> tuple[dict, int, str, str]:
    """해당 월의 1일부터 date까지 모든 데이터를 합산. 공판장 포함 날짜만 사용.

    반환: (monthly_corp_data, loaded_days, first_date, last_date)
    """
    from datetime import timedelta

    dt = datetime.strptime(date, "%Y-%m-%d")
    month_start = dt.replace(day=1)

    monthly = defaultdict(lambda: {
        "count": 0, "total_kg": 0, "amount": 0, "products": defaultdict(lambda: {
            "count": 0, "prices": [], "total_kg": 0, "amount": 0, "origins": Counter()
        })
    })

    loaded_days = 0
    first_loaded = None
    last_loaded = None

    current = month_start
    while current <= dt:
        d_str = current.strftime("%Y-%m-%d")
        data = load_data(d_str)
        if data:
            loaded_days += 1
            if not first_loaded:
                first_loaded = d_str
            last_loaded = d_str
            for m in data["markets"].values():
                for item in m["items"]:
                    market = item["market_name"]
                    corp = item["corp_name"]
                    key = f"{market}|{corp}"
                    product = item["product"]
                    unit_wt = item.get("unit_weight", 0)
                    price = item.get("price", 0)
                    qty = item["quantity"] if isinstance(item["quantity"], (int, float)) else 0
                    amount = price * qty
                    kg = unit_wt * qty
                    per_kg = amount / kg if kg > 0 else 0

                    monthly[key]["count"] += 1
                    monthly[key]["total_kg"] += kg
                    monthly[key]["amount"] += amount
                    monthly[key]["products"][product]["count"] += 1
                    monthly[key]["products"][product]["total_kg"] += kg
                    monthly[key]["products"][product]["amount"] += amount
                    if per_kg > 0:
                        monthly[key]["products"][product]["prices"].append(per_kg)
                    origin = item.get("origin", "-")
                    if origin and origin != "-":
                        monthly[key]["products"][product]["origins"][origin] += 1
        current += timedelta(days=1)

    return monthly, loaded_days, first_loaded or date[:8] + "01", last_loaded or date


def generate_djc_report(date: str) -> str:
    data = load_data(date)
    if not data:
        return f"{date} 데이터가 없습니다."

    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]

    # ── 당일 전체 법인별 집계 ──
    corp_data = _aggregate_data(data)

    # ── 우리 법인 찾기 ──
    our_key = None
    for k in corp_data:
        if OUR_CORP in k and OUR_MARKET in k:
            our_key = k
            break

    if not our_key:
        return _generate_fallback_report(corp_data, date, weekday)

    our = corp_data[our_key]

    # ── 월간 누적 집계 (대전 비교용) ──
    monthly_data, monthly_days, monthly_first, monthly_last = _aggregate_monthly(date)
    monthly_label = f"{monthly_first} ~ {monthly_last} ({monthly_days}일)"

    monthly_dj = {
        k: v for k, v in monthly_data.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }
    total_monthly_dj_kg = sum(v["total_kg"] for v in monthly_dj.values())
    total_monthly_dj_amount = sum(v["amount"] for v in monthly_dj.values())

    # ── 당일 대전 (핵심 지표용 — 공판장 포함 데이터) ──
    compare_date, compare_data = _find_complete_date(date)
    use_compare = compare_date != date and compare_data is not None

    if use_compare:
        dj_source = _aggregate_data(compare_data)
    else:
        dj_source = corp_data
    daejeon_today = {
        k: v for k, v in dj_source.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }
    total_dj_kg = sum(v["total_kg"] for v in daejeon_today.values())
    total_dj_amount = sum(v["amount"] for v in daejeon_today.values())

    # ── 전국 법인 순위 (금액 순) ──
    all_corps_ranked = sorted(corp_data.items(), key=lambda x: x[1]["amount"], reverse=True)
    our_national_rank = next(
        (i for i, (k, _) in enumerate(all_corps_ranked, 1) if k == our_key), 0
    )

    total_nat_kg = sum(v["total_kg"] for v in corp_data.values())
    total_nat_amount = sum(v["amount"] for v in corp_data.values())

    lines = []

    # ═══════════════════════════════════════════
    # 헤더
    # ═══════════════════════════════════════════
    lines.append(f"# 대전중앙청과㈜ 경영 분석 리포트")
    lines.append(f"**{date} ({weekday}) 정산 기준**\n")
    lines.append(f"> 🟠 = 월간 누적 ({monthly_label}, 공판장 포함) | 🔵 = 당일({date}) 정산 데이터\n")

    # ═══════════════════════════════════════════
    # 1. 대전 지역 법인 경쟁 비교 — 월간 누적 (🟠)
    # ═══════════════════════════════════════════
    lines.append("---")
    lines.append(f"## 1. 대전 지역 법인 경쟁 비교 🟠 ({monthly_label})\n")
    lines.append(f"> 📊 {dt.month}월 누적 데이터 (공판장 포함, {monthly_days}일 합산)\n")

    dj_sorted = sorted(monthly_dj.items(), key=lambda x: x[1]["amount"], reverse=True)

    lines.append("| 순위 | 시장 | 법인 | 거래건수 | 물량(톤) | 금액(만원) | 품목수 | 점유율(물량) | 점유율(금액) |")
    lines.append("|------|------|------|---------|---------|----------|--------|----------|------------|")

    our_monthly_key = None
    for k in monthly_dj:
        if OUR_CORP in k and OUR_MARKET in k:
            our_monthly_key = k
            break

    for i, (k, v) in enumerate(dj_sorted, 1):
        market, corp = k.split("|")
        kg_share = v["total_kg"] / total_monthly_dj_kg * 100 if total_monthly_dj_kg else 0
        amount_share = v["amount"] / total_monthly_dj_amount * 100 if total_monthly_dj_amount else 0
        marker = " ⭐" if (OUR_CORP in k and OUR_MARKET in k) else ""
        lines.append(
            f"| {i} | {market} | {corp}{marker} | {v['count']:,} | "
            f"{v['total_kg']/1000:,.1f} | {v['amount']/10000:,.0f} | {len(v['products'])} | {kg_share:.1f}% | {amount_share:.1f}% |"
        )

    # 당일 대전 법인 비교
    dj_today_sorted = sorted(daejeon_today.items(), key=lambda x: x[1]["amount"], reverse=True)
    dj_today_date = compare_date if use_compare else date
    lines.append(f"\n### 당일 대전 법인 비교 ({dj_today_date})\n")

    lines.append("| 순위 | 시장 | 법인 | 거래건수 | 물량(톤) | 금액(만원) | 점유율(물량) | 점유율(금액) |")
    lines.append("|------|------|------|---------|---------|----------|----------|------------|")
    for i, (k, v) in enumerate(dj_today_sorted, 1):
        market, corp = k.split("|")
        kg_share = v["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
        amount_share = v["amount"] / total_dj_amount * 100 if total_dj_amount else 0
        marker = " ⭐" if (OUR_CORP in k and OUR_MARKET in k) else ""
        lines.append(
            f"| {i} | {market} | {corp}{marker} | {v['count']:,} | "
            f"{v['total_kg']/1000:,.1f} | {v['amount']/10000:,.0f} | {kg_share:.1f}% | {amount_share:.1f}% |"
        )
    if use_compare:
        lines.append(f"\n> ⚠️ 공판장 정산 지연으로 {compare_date} 데이터 기준\n")

    # 월간 대전 내 품목별 점유율 비교 (주요 품목 60개 — 금액 순)
    lines.append(f"\n### 대전 주요 품목별 법인 점유율 (월간 {monthly_days}일 누적, 금액 순)\n")

    dj_product_total_amount = Counter()
    dj_product_total_count = Counter()
    dj_product_by_corp_amount = defaultdict(Counter)
    dj_product_by_corp_count = defaultdict(Counter)
    for k, v in monthly_dj.items():
        _, corp = k.split("|")
        for product, ps in v["products"].items():
            dj_product_total_amount[product] += ps["amount"]
            dj_product_total_count[product] += ps["count"]
            dj_product_by_corp_amount[product][corp] += ps["amount"]
            dj_product_by_corp_count[product][corp] += ps["count"]

    top_dj_products = dj_product_total_amount.most_common(60)

    # 헤더: 법인명 축약
    corp_names = []
    for k, _ in dj_sorted:
        _, corp = k.split("|")
        corp_names.append(corp)
    lines.append("| 순위 | 품목 | 대전전체(만원) | " + " | ".join(corp_names) + " |")
    lines.append("|------|------|------------|" + "|".join("--------|" for _ in dj_sorted))

    for rank, (product, total_amt) in enumerate(top_dj_products, 1):
        row = f"| {rank} | {product} | {total_amt/10000:,.0f} |"
        for k, _ in dj_sorted:
            _, corp = k.split("|")
            amt = dj_product_by_corp_amount[product].get(corp, 0)
            pct = amt / total_amt * 100 if total_amt else 0
            if OUR_CORP in k and OUR_MARKET in k and amt > 0:
                row += f" **{amt/10000:,.0f}**({pct:.0f}%) |"
            elif amt > 0:
                row += f" {amt/10000:,.0f}({pct:.0f}%) |"
            else:
                row += " - |"
        lines.append(row)

    # ═══════════════════════════════════════════
    # 2. 핵심 지표 (🔵 당일)
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append(f"## 2. 오늘의 핵심 지표 🔵 ({date})\n")

    our_share_nat_kg = our["total_kg"] / total_nat_kg * 100 if total_nat_kg else 0
    our_amount_man = our["amount"] / 10000

    if use_compare:
        our_cmp_key = None
        for k in dj_source:
            if OUR_CORP in k and OUR_MARKET in k:
                our_cmp_key = k
                break
        our_cmp = dj_source[our_cmp_key] if our_cmp_key else our
        our_share_dj_kg = our_cmp["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
        our_amount_share_dj = our_cmp["amount"] / total_dj_amount * 100 if total_dj_amount else 0
    else:
        our_share_dj_kg = our["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
        our_amount_share_dj = our["amount"] / total_dj_amount * 100 if total_dj_amount else 0

    dj_note = f" ({compare_date} 기준)" if use_compare else ""

    lines.append(f"| 지표 | 수치 |")
    lines.append(f"|------|------|")
    lines.append(f"| 총 거래건수 | **{our['count']:,}건** |")
    lines.append(f"| 총 물량 | **{our['total_kg']/1000:,.1f}톤** ({our['total_kg']:,.0f}kg) |")
    lines.append(f"| 총 거래금액 | **{our_amount_man:,.0f}만원** |")
    lines.append(f"| 취급 품목 수 | {len(our['products']):,}개 |")
    lines.append(f"| 대전 점유율(물량) | **{our_share_dj_kg:.1f}%** (대전 전체 {total_dj_kg/1000:,.1f}톤{dj_note}) |")
    lines.append(f"| 대전 점유율(금액) | **{our_amount_share_dj:.1f}%** (대전 전체 {total_dj_amount/10000:,.0f}만원{dj_note}) |")
    lines.append(f"| 전국 순위(금액) | **{our_national_rank}위** / {len(corp_data)}개 법인 |")
    lines.append(f"| 전국 점유율(물량) | {our_share_nat_kg:.1f}% ({total_nat_kg/1000:,.1f}톤 중) |")

    # ═══════════════════════════════════════════
    # 3. 우리 법인 품목 상세 (🔵 당일, 금액 순, 50개)
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append(f"## 3. 우리 법인 품목별 상세 🔵 ({date})\n")

    our_products = sorted(
        our["products"].items(), key=lambda x: x[1]["amount"], reverse=True
    )

    lines.append("| 순위 | 품목 | 건수 | 물량(kg) | 금액(만원) | 평균(원/kg) | 범위 | 주요 산지 |")
    lines.append("|------|------|------|---------|----------|-----------|------|----------|")

    for i, (product, ps) in enumerate(our_products[:50], 1):
        prices = _filter_outliers(ps["prices"])
        if not prices:
            avg = mn = mx = 0
        else:
            avg = sum(prices) / len(prices)
            mn = min(prices)
            mx = max(prices)

        top_origin = ""
        if ps["origins"]:
            top_o = ps["origins"].most_common(2)
            top_origin = ", ".join(f"{o}({c})" for o, c in top_o)

        lines.append(
            f"| {i} | {product} | {ps['count']:,} | {ps['total_kg']:,.0f} | "
            f"{ps['amount']/10000:,.0f} | {avg:,.0f} | {mn:,.0f}~{mx:,.0f} | {top_origin} |"
        )

    # ═══════════════════════════════════════════
    # 4. 전국 가격 경쟁력 (주요 품목, 20개)
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append(f"## 4. 전국 가격 경쟁력 비교 (주요 품목) 🔵 ({date})\n")
    lines.append("우리 법인 평균가 vs 전국 평균가 비교\n")

    lines.append("| 품목 | 우리(원/kg) | 전국평균 | 차이 | 전국 최저법인 | 전국 최고법인 |")
    lines.append("|------|-----------|---------|------|------------|------------|")

    price_count = 0
    for product, ps in our_products:
        if price_count >= 20:
            break
        our_prices = _filter_outliers(ps["prices"])
        if not our_prices:
            continue
        our_avg = sum(our_prices) / len(our_prices)

        nat_prices_by_corp = {}
        for k, v in corp_data.items():
            if product in v["products"]:
                cp = _filter_outliers(v["products"][product]["prices"])
                if cp:
                    _, corp = k.split("|")
                    nat_prices_by_corp[corp] = sum(cp) / len(cp)

        if not nat_prices_by_corp:
            continue

        all_nat_prices = list(nat_prices_by_corp.values())
        nat_avg = sum(all_nat_prices) / len(all_nat_prices)
        diff = ((our_avg - nat_avg) / nat_avg) * 100

        lowest_corp = min(nat_prices_by_corp, key=nat_prices_by_corp.get)
        highest_corp = max(nat_prices_by_corp, key=nat_prices_by_corp.get)

        arrow = "🔺" if diff > 3 else ("🔻" if diff < -3 else "➖")
        lines.append(
            f"| {product} | {our_avg:,.0f} | {nat_avg:,.0f} | "
            f"{arrow} {diff:+.1f}% | {lowest_corp}({nat_prices_by_corp[lowest_corp]:,.0f}) | "
            f"{highest_corp}({nat_prices_by_corp[highest_corp]:,.0f}) |"
        )
        price_count += 1

    # ═══════════════════════════════════════════
    # 5. 산지 분포
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append(f"## 5. 산지 분포 (전체) 🔵 ({date})\n")

    all_origins = Counter()
    for ps in our["products"].values():
        all_origins.update(ps["origins"])

    lines.append("| 순위 | 산지 | 건수 | 비중 |")
    lines.append("|------|------|------|------|")
    for i, (origin, cnt) in enumerate(all_origins.most_common(20), 1):
        pct = cnt / our["count"] * 100
        lines.append(f"| {i} | {origin} | {cnt:,} | {pct:.1f}% |")

    # ═══════════════════════════════════════════
    # 6. 전국 법인 순위 — 금액(만원) 순
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append(f"## 6. 전국 법인 순위 (우리 근처, 금액 순) 🔵 ({date})\n")

    start = max(0, our_national_rank - 4)
    end = min(len(all_corps_ranked), our_national_rank + 4)

    lines.append("| 순위 | 시장 | 법인 | 거래건수 | 물량(톤) | 금액(만원) | 품목수 |")
    lines.append("|------|------|------|---------|---------|----------|--------|")
    for i, (k, v) in enumerate(all_corps_ranked[start:end], start + 1):
        market, corp = k.split("|")
        marker = " ⭐" if k == our_key else ""
        lines.append(f"| {i} | {market} | {corp}{marker} | {v['count']:,} | {v['total_kg']/1000:,.1f} | {v['amount']/10000:,.0f} | {len(v['products'])} |")

    # ═══════════════════════════════════════════
    # 7. 경영 시사점
    # ═══════════════════════════════════════════
    lines.append("\n---")
    lines.append("## 7. 경영 시사점\n")

    # 월간 기준 대전 1위 법인 대비
    if dj_sorted:
        dj_top_key, dj_top = dj_sorted[0]
        _, dj_top_corp = dj_top_key.split("|")
        our_monthly = monthly_dj.get(our_monthly_key, {"total_kg": 0, "amount": 0})
        our_m_kg_share = our_monthly["total_kg"] / total_monthly_dj_kg * 100 if total_monthly_dj_kg else 0
        dj_top_kg_share = dj_top["total_kg"] / total_monthly_dj_kg * 100 if total_monthly_dj_kg else 0
        kg_gap = (dj_top["total_kg"] - our_monthly["total_kg"]) / 1000
        amount_gap = (dj_top["amount"] - our_monthly["amount"]) / 10000
        lines.append(f"- 월간 대전 1위 {dj_top_corp} 대비 **{kg_gap:,.1f}톤 / {amount_gap:,.0f}만원 차이** (물량 점유율 {our_m_kg_share:.1f}% vs {dj_top_kg_share:.1f}%)")

    # 우리만 취급하는 품목 / 우리가 안 취급하는 품목 (월간 기준)
    our_monthly_products = set(monthly_dj.get(our_monthly_key, {}).get("products", {}).keys()) if our_monthly_key else set()
    for k, v in dj_sorted:
        if OUR_CORP in k and OUR_MARKET in k:
            continue
        _, corp = k.split("|")
        other_products = set(v["products"].keys())
        only_them = other_products - our_monthly_products
        if only_them:
            top_only = sorted(only_them, key=lambda p: v["products"][p]["amount"], reverse=True)[:5]
            items_str = ", ".join(f"{p}({v['products'][p]['amount']/10000:,.0f}만원)" for p in top_only)
            lines.append(f"- {corp}만 취급 (우리 미취급): {items_str}")

    # 데이터 안내
    lines.append(f"\n---")
    lines.append(f"\n### 📋 데이터 안내")
    lines.append(f"- **당일 정산 (🔵)**: {date} 기준 ({len(corp_data)}개 법인)")
    lines.append(f"- **월간 누적 (🟠)**: {monthly_label} ({monthly_days}일, 공판장 포함)")
    if use_compare:
        lines.append(f"- **당일 대전 점유율**: {compare_date} 기준 (공판장 포함 최신 데이터)")
    lines.append(f"- 농협/원협 공판장은 정산 업로드가 1~2일 늦으므로, 월간 누적에는 이미 반영됩니다.")

    lines.append(f"\n*자동 생성 by 송봇 | data.go.kr 정산 데이터 기준*")

    return "\n".join(lines)


DASHBOARD_URL = "https://songt-50.github.io/wholesale-dashboard/"


def generate_telegram_summary(date: str) -> str:
    """텔레그램용 DJC 경영 리포트 요약 (4096자 이내)"""
    data = load_data(date)
    if not data:
        return f"📊 {date} DJC 경영 리포트: 데이터 없음"

    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]
    corp_data = _aggregate_data(data)

    our_key = None
    for k in corp_data:
        if OUR_CORP in k and OUR_MARKET in k:
            our_key = k
            break
    if not our_key:
        return f"📊 {date}({weekday}) 대전중앙청과 데이터 없음"

    our = corp_data[our_key]

    # 월간 누적
    monthly_data, monthly_days, monthly_first, monthly_last = _aggregate_monthly(date)
    monthly_dj = {
        k: v for k, v in monthly_data.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }
    total_m_dj_kg = sum(v["total_kg"] for v in monthly_dj.values())
    total_m_dj_amt = sum(v["amount"] for v in monthly_dj.values())
    dj_m_sorted = sorted(monthly_dj.items(), key=lambda x: x[1]["amount"], reverse=True)

    # 당일 대전
    compare_date, compare_data = _find_complete_date(date)
    use_compare = compare_date != date and compare_data is not None
    if use_compare:
        dj_source = _aggregate_data(compare_data)
    else:
        dj_source = corp_data
    dj_today = {
        k: v for k, v in dj_source.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }
    total_dj_kg = sum(v["total_kg"] for v in dj_today.values())
    total_dj_amt = sum(v["amount"] for v in dj_today.values())
    dj_t_sorted = sorted(dj_today.items(), key=lambda x: x[1]["amount"], reverse=True)

    # 전국 순위
    all_ranked = sorted(corp_data.items(), key=lambda x: x[1]["amount"], reverse=True)
    nat_rank = next((i for i, (k, _) in enumerate(all_ranked, 1) if k == our_key), 0)

    msg = []
    msg.append(f"📊 대전중앙청과㈜ 경영 분석")
    msg.append(f"📅 {date} ({weekday}) 정산 기준\n")

    # 핵심 지표
    msg.append(f"━━ 오늘 핵심 ━━")
    msg.append(f"거래: {our['count']:,}건 | {our['total_kg']/1000:,.1f}톤 | {our['amount']/10000:,.0f}만원")
    msg.append(f"전국 {nat_rank}위/{len(corp_data)}법인 | 품목 {len(our['products'])}개\n")

    # 월간 대전 비교
    msg.append(f"━━ 🟠 대전 월간 ({monthly_first}~{monthly_last}, {monthly_days}일) ━━")
    for i, (k, v) in enumerate(dj_m_sorted, 1):
        _, corp = k.split("|")
        kg_s = v["total_kg"] / total_m_dj_kg * 100 if total_m_dj_kg else 0
        amt_s = v["amount"] / total_m_dj_amt * 100 if total_m_dj_amt else 0
        star = "⭐" if (OUR_CORP in k and OUR_MARKET in k) else f"{i}위"
        msg.append(f"  {star} {corp}: {v['total_kg']/1000:,.0f}톤({kg_s:.0f}%) {v['amount']/10000:,.0f}만원({amt_s:.0f}%)")
    msg.append("")

    # 당일 대전 비교
    dj_date = compare_date if use_compare else date
    msg.append(f"━━ 🔵 대전 당일 ({dj_date}) ━━")
    for i, (k, v) in enumerate(dj_t_sorted, 1):
        _, corp = k.split("|")
        kg_s = v["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
        amt_s = v["amount"] / total_dj_amt * 100 if total_dj_amt else 0
        star = "⭐" if (OUR_CORP in k and OUR_MARKET in k) else f"{i}위"
        msg.append(f"  {star} {corp}: {v['total_kg']/1000:,.0f}톤({kg_s:.0f}%) {v['amount']/10000:,.0f}만원({amt_s:.0f}%)")
    msg.append("")

    # 주요 품목 Top 10 (금액 순)
    our_products = sorted(our["products"].items(), key=lambda x: x[1]["amount"], reverse=True)
    msg.append(f"━━ 오늘 주요 품목 (금액 순) ━━")
    for i, (product, ps) in enumerate(our_products[:10], 1):
        msg.append(f"  {i}. {product}: {ps['total_kg']:,.0f}kg {ps['amount']/10000:,.0f}만원 ({ps['count']}건)")
    msg.append("")

    msg.append(f"📧 전체 리포트: 이메일 확인")
    msg.append(f"🗺 대시보드: {DASHBOARD_URL}")
    msg.append(f"\n🤖 자동 생성 by 송봇")

    return "\n".join(msg)


def send_djc_telegram(date: str):
    """DJC 경영 리포트 요약을 텔레그램으로 발송"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        print("TELEGRAM 설정 없음 — 텔레그램 건너뜀")
        return

    import httpx
    text = generate_telegram_summary(date)
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    resp = httpx.post(url, json={
        "chat_id": chat_id,
        "text": text,
    }, timeout=15)

    if resp.status_code == 200:
        print(f"DJC 텔레그램 발송 완료")
    else:
        print(f"DJC 텔레그램 발송 실패: {resp.status_code} {resp.text}")


def md_to_html(md: str) -> str:
    lines = md.split("\n")
    out = []
    in_table = False
    section_color = "blue"  # "blue" = 당일정산, "orange" = 공판장포함
    for line in lines:
        if line.startswith("### "):
            if in_table:
                out.append("</table><br>")
                in_table = False
            h3_color = "#e65100" if section_color == "orange" else "#1565c0"
            h3_border = "#ff9800" if section_color == "orange" else "#1565c0"
            out.append(
                f'<h3 style="margin:18px 0 5px;color:{h3_color};'
                f'border-bottom:2px solid {h3_border};padding-bottom:4px;">'
                f"{line[4:]}</h3>"
            )
        elif line.startswith("## "):
            if in_table:
                out.append("</table><br>")
                in_table = False
            heading = line[3:]
            if "🟠" in heading:
                section_color = "orange"
                out.append(f'<h2 style="margin:22px 0 8px;color:#e65100;border-left:4px solid #ff9800;padding-left:8px;">{heading}</h2>')
            elif "🔵" in heading:
                section_color = "blue"
                out.append(f'<h2 style="margin:22px 0 8px;color:#0d47a1;border-left:4px solid #1976d2;padding-left:8px;">{heading}</h2>')
            else:
                out.append(f'<h2 style="margin:22px 0 8px;color:#0d47a1;">{heading}</h2>')
        elif line.startswith("# "):
            out.append(f'<h1 style="margin:0 0 5px;color:#0d47a1;">{line[2:]}</h1>')
        elif line.startswith("| ") and "---" not in line:
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if not in_table:
                th_bg = "#e65100" if section_color == "orange" else "#0d47a1"
                out.append(
                    '<table border="1" cellpadding="5" '
                    'style="border-collapse:collapse;font-size:12px;width:100%;">'
                )
                out.append(
                    f"<tr style='background:{th_bg};color:white;'>"
                    + "".join(f"<th>{c}</th>" for c in cells)
                    + "</tr>"
                )
                in_table = True
            else:
                row_style = ""
                raw = "|".join(cells)
                if "⭐" in raw:
                    row_style = " style='background:#e3f2fd;font-weight:bold;'"
                out.append(
                    f"<tr{row_style}>"
                    + "".join(f"<td>{c}</td>" for c in cells)
                    + "</tr>"
                )
        elif line.startswith("|") and "---" in line:
            continue
        elif line.startswith("---"):
            if in_table:
                out.append("</table>")
                in_table = False
            out.append("<hr>")
        else:
            if in_table:
                out.append("</table>")
                in_table = False
            if line.startswith("- "):
                line_html = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line[2:])
                out.append(f"<li style='margin:3px 0;'>{line_html}</li>")
            elif line.strip():
                line_html = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line)
                out.append(f"<p style='margin:3px 0;'>{line_html}</p>")
    if in_table:
        out.append("</table>")
    return "\n".join(out)


def send_email(report_md: str, date: str):
    gmail_addr = os.getenv("GMAIL_ADDRESS", "")
    gmail_pw = os.getenv("GMAIL_APP_PASSWORD", "")
    if not gmail_addr or not gmail_pw:
        print("Gmail 설정 없음 — 이메일 건너뜀")
        return

    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]

    html_body = f"""
<div style="font-family:'Apple SD Gothic Neo','Noto Sans KR',sans-serif;max-width:850px;margin:0 auto;">
    <div style="background:#0d47a1;color:white;padding:20px;border-radius:8px 8px 0 0;">
        <h1 style="margin:0;font-size:20px;">📊 대전중앙청과㈜ 경영 분석 리포트</h1>
        <p style="margin:5px 0 0;color:#90caf9;">{date} ({weekday}) 정산 기준 | 대전 4개 법인 + 전국 비교</p>
    </div>
    <div style="padding:20px;background:#fff;border:1px solid #ddd;">
        {md_to_html(report_md)}
    </div>
    <div style="padding:15px;background:#e3f2fd;border-radius:0 0 8px 8px;font-size:12px;color:#666;">
        data.go.kr 정산 데이터 기준 (원/kg) | 자동 생성 by 송봇 (삽질코딩)
    </div>
</div>
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[대전중앙청과] {date}({weekday}) 경영 분석 리포트"
    msg["From"] = gmail_addr
    msg["To"] = gmail_addr
    msg.attach(MIMEText(report_md, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_addr, gmail_pw)
            server.send_message(msg)
        print(f"이메일 발송 완료: {gmail_addr}")
    except Exception as e:
        print(f"이메일 발송 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description="대전중앙청과 경영 분석 리포트")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--email", action="store_true", help="이메일 발송")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 요약 발송")
    args = parser.parse_args()

    print(f"대전중앙청과㈜ 경영 분석 리포트 생성: {args.date}")
    report = generate_djc_report(args.date)

    # 저장
    REPORT_DIR.mkdir(exist_ok=True)
    out = REPORT_DIR / f"djc_management_{args.date}.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"저장: {out}")

    if args.email:
        send_email(report, args.date)

    if args.telegram:
        send_djc_telegram(args.date)

    print("\n" + report)


if __name__ == "__main__":
    main()
