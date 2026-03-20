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


def generate_djc_report(date: str) -> str:
    data = load_data(date)
    if not data:
        return f"{date} 데이터가 없습니다."

    # 공판장 포함 데이터 탐색 (대전 비교용)
    compare_date, compare_data = _find_complete_date(date)
    use_compare = compare_date != date and compare_data is not None

    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]

    # ── 전체 법인별 집계 ──
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

    # ── 우리 법인 찾기 ──
    our_key = None
    for k in corp_data:
        if OUR_CORP in k and OUR_MARKET in k:
            our_key = k
            break

    if not our_key:
        # 우리 법인 없어도 대전 다른 법인 현황은 보여주기
        return _generate_fallback_report(corp_data, date, weekday)

    our = corp_data[our_key]

    # ── 대전 지역 법인들 (공판장 포함 데이터 사용) ──
    if use_compare:
        # 공판장 정산 지연으로 당일 데이터 불완전 → D-N 데이터로 대전 비교
        compare_corp_data = defaultdict(lambda: {
            "count": 0, "total_kg": 0, "amount": 0, "products": defaultdict(lambda: {
                "count": 0, "prices": [], "total_kg": 0, "amount": 0, "origins": Counter()
            })
        })
        for m in compare_data["markets"].values():
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
                compare_corp_data[key]["count"] += 1
                compare_corp_data[key]["total_kg"] += kg
                compare_corp_data[key]["amount"] += amount
                compare_corp_data[key]["products"][product]["count"] += 1
                compare_corp_data[key]["products"][product]["total_kg"] += kg
                compare_corp_data[key]["products"][product]["amount"] += amount
                if per_kg > 0:
                    compare_corp_data[key]["products"][product]["prices"].append(per_kg)
        dj_source = compare_corp_data
    else:
        dj_source = corp_data

    daejeon_corps = {
        k: v for k, v in dj_source.items()
        if any(dm in k for dm in DAEJEON_MARKETS)
    }

    # ── 전국 법인 순위 ──
    all_corps_ranked = sorted(corp_data.items(), key=lambda x: x[1]["count"], reverse=True)
    our_national_rank = next(
        (i for i, (k, _) in enumerate(all_corps_ranked, 1) if k == our_key), 0
    )

    total_national = sum(v["count"] for v in corp_data.values())
    total_daejeon = sum(v["count"] for v in daejeon_corps.values())

    lines = []

    # ═══════════════════════════════════════════
    # 헤더
    # ═══════════════════════════════════════════
    lines.append(f"# 대전중앙청과㈜ 경영 분석 리포트")
    lines.append(f"**{date} ({weekday}) 정산 기준**\n")

    if use_compare:
        lines.append(f"> 🔵 = 당일({date}) 정산 데이터 | 🟠 = {compare_date} 정산 데이터 (공판장 포함)\n")

    # ═══════════════════════════════════════════
    # 1. 핵심 지표
    # ═══════════════════════════════════════════
    lines.append("---")
    date_tag = f" 🔵 ({date})" if use_compare else ""
    lines.append(f"## 1. 오늘의 핵심 지표{date_tag}\n")

    total_nat_kg = sum(v["total_kg"] for v in corp_data.values())
    our_share_nat_kg = our["total_kg"] / total_nat_kg * 100 if total_nat_kg else 0
    our_amount_man = our["amount"] / 10000  # 만원 단위
    total_nat_amount = sum(v["amount"] for v in corp_data.values())

    # 대전 점유율은 공판장 포함 데이터 기준
    total_dj_amount = sum(v["amount"] for v in daejeon_corps.values())
    total_dj_kg = sum(v["total_kg"] for v in daejeon_corps.values())
    if use_compare:
        # compare_data 기준 우리 법인 찾기
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

    lines.append(f"| 지표 | 수치 |")
    lines.append(f"|------|------|")
    our_ton = our["total_kg"] / 1000
    lines.append(f"| 총 거래건수 | **{our['count']:,}건** |")
    lines.append(f"| 총 물량 | **{our_ton:,.1f}톤** ({our['total_kg']:,.0f}kg) |")
    lines.append(f"| 총 거래금액 | **{our_amount_man:,.0f}만원** |")
    lines.append(f"| 취급 품목 수 | {len(our['products']):,}개 |")
    dj_note = f" ({compare_date} 기준)" if use_compare else ""
    lines.append(f"| 대전 점유율(물량) | **{our_share_dj_kg:.1f}%** (대전 전체 {total_dj_kg/1000:,.1f}톤{dj_note}) |")
    lines.append(f"| 대전 점유율(금액) | **{our_amount_share_dj:.1f}%** (대전 전체 {total_dj_amount/10000:,.0f}만원{dj_note}) |")
    lines.append(f"| 전국 순위 | **{our_national_rank}위** / {len(corp_data)}개 법인 |")
    lines.append(f"| 전국 점유율(물량) | {our_share_nat_kg:.1f}% ({total_nat_kg/1000:,.1f}톤 중) |")

    # ═══════════════════════════════════════════
    # 2. 대전 지역 경쟁 비교
    # ═══════════════════════════════════════════
    lines.append("\n---")
    if use_compare:
        lines.append(f"## 2. 대전 지역 법인 경쟁 비교 🟠 ({compare_date} 기준)\n")
        lines.append(f"> ⚠️ 공판장 정산 지연으로 {compare_date} 데이터 기준 비교\n")
    else:
        lines.append("## 2. 대전 지역 법인 경쟁 비교\n")

    dj_sorted = sorted(daejeon_corps.items(), key=lambda x: x[1]["count"], reverse=True)

    lines.append("| 순위 | 시장 | 법인 | 거래건수 | 물량(톤) | 금액(만원) | 품목수 | 점유율(물량) | 점유율(금액) |")
    lines.append("|------|------|------|---------|---------|----------|--------|----------|------------|")

    for i, (k, v) in enumerate(dj_sorted, 1):
        market, corp = k.split("|")
        kg_share = v["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
        amount_share = v["amount"] / total_dj_amount * 100 if total_dj_amount else 0
        marker = " ⭐" if k == our_key else ""
        lines.append(
            f"| {i} | {market} | {corp}{marker} | {v['count']:,} | "
            f"{v['total_kg']/1000:,.1f} | {v['amount']/10000:,.0f} | {len(v['products'])} | {kg_share:.1f}% | {amount_share:.1f}% |"
        )

    # 대전 내 품목별 점유율 비교 (주요 품목)
    lines.append("\n### 대전 주요 품목별 법인 점유율\n")

    # 대전 전체 품목 집계
    dj_product_total = Counter()
    dj_product_by_corp = defaultdict(Counter)
    for k, v in daejeon_corps.items():
        _, corp = k.split("|")
        for product, ps in v["products"].items():
            dj_product_total[product] += ps["count"]
            dj_product_by_corp[product][corp] += ps["count"]

    top_dj_products = dj_product_total.most_common(15)

    lines.append("| 품목 | 대전전체 | " + " | ".join(
        corp.split("|")[1] if "|" in corp else corp
        for corp, _ in dj_sorted
    ) + " |")
    lines.append("|------|---------|" + "|".join("--------|" for _ in dj_sorted))

    for product, total_cnt in top_dj_products:
        row = f"| {product} | {total_cnt:,} |"
        for k, _ in dj_sorted:
            _, corp = k.split("|")
            cnt = dj_product_by_corp[product].get(corp, 0)
            pct = cnt / total_cnt * 100 if total_cnt else 0
            if k == our_key and cnt > 0:
                row += f" **{cnt}**({pct:.0f}%) |"
            elif cnt > 0:
                row += f" {cnt}({pct:.0f}%) |"
            else:
                row += " - |"
        lines.append(row)

    # ═══════════════════════════════════════════
    # 3. 우리 법인 품목 상세
    # ═══════════════════════════════════════════
    lines.append("\n---")
    date_tag3 = f" 🔵 ({date})" if use_compare else ""
    lines.append(f"## 3. 우리 법인 품목별 상세{date_tag3}\n")

    our_products = sorted(
        our["products"].items(), key=lambda x: x[1]["count"], reverse=True
    )

    lines.append("| 순위 | 품목 | 건수 | 물량(kg) | 금액(만원) | 평균(원/kg) | 범위 | 주요 산지 |")
    lines.append("|------|------|------|---------|----------|-----------|------|----------|")

    for i, (product, ps) in enumerate(our_products[:25], 1):
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
    # 4. 전국 가격 경쟁력 (주요 품목)
    # ═══════════════════════════════════════════
    lines.append("\n---")
    date_tag4 = f" 🔵 ({date})" if use_compare else ""
    lines.append(f"## 4. 전국 가격 경쟁력 비교 (주요 품목){date_tag4}\n")
    lines.append("우리 법인 평균가 vs 전국 평균가 비교\n")

    lines.append("| 품목 | 우리(원/kg) | 전국평균 | 차이 | 전국 최저법인 | 전국 최고법인 |")
    lines.append("|------|-----------|---------|------|------------|------------|")

    for product, ps in our_products[:15]:
        our_prices = _filter_outliers(ps["prices"])
        if not our_prices:
            continue
        our_avg = sum(our_prices) / len(our_prices)

        # 전국 같은 품목 수집
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

    # ═══════════════════════════════════════════
    # 5. 산지 집중도 분석
    # ═══════════════════════════════════════════
    lines.append("\n---")
    date_tag5 = f" 🔵 ({date})" if use_compare else ""
    lines.append(f"## 5. 산지 분포 (전체){date_tag5}\n")

    all_origins = Counter()
    for ps in our["products"].values():
        all_origins.update(ps["origins"])

    lines.append("| 순위 | 산지 | 건수 | 비중 |")
    lines.append("|------|------|------|------|")
    for i, (origin, cnt) in enumerate(all_origins.most_common(20), 1):
        pct = cnt / our["count"] * 100
        lines.append(f"| {i} | {origin} | {cnt:,} | {pct:.1f}% |")

    # ═══════════════════════════════════════════
    # 6. 전국 순위 (근처 법인)
    # ═══════════════════════════════════════════
    lines.append("\n---")
    date_tag6 = f" 🔵 ({date})" if use_compare else ""
    lines.append(f"## 6. 전국 법인 순위 (우리 근처){date_tag6}\n")

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

    # 대전 1위 법인 대비
    dj_top_key, dj_top = dj_sorted[0]
    _, dj_top_corp = dj_top_key.split("|")
    kg_gap = (dj_top["total_kg"] - our["total_kg"]) / 1000
    amount_gap = (dj_top["amount"] - our["amount"]) / 10000
    dj_top_kg_share = dj_top["total_kg"] / total_dj_kg * 100 if total_dj_kg else 0
    lines.append(f"- 대전 1위 {dj_top_corp} 대비 **{kg_gap:,.1f}톤 / {amount_gap:,.0f}만원 차이** (물량 점유율 {our_share_dj_kg:.1f}% vs {dj_top_kg_share:.1f}%)")

    # 우리만 취급하는 품목 / 우리가 안 취급하는 품목
    our_products_set = set(our["products"].keys())
    for k, v in dj_sorted:
        if k == our_key:
            continue
        _, corp = k.split("|")
        other_products = set(v["products"].keys())
        only_them = other_products - our_products_set
        if only_them:
            top_only = sorted(only_them, key=lambda p: v["products"][p]["count"], reverse=True)[:5]
            items_str = ", ".join(f"{p}({v['products'][p]['count']}건)" for p in top_only)
            lines.append(f"- {corp}만 취급 (우리 미취급): {items_str}")

    # 데이터 안내 섹션
    if use_compare:
        lines.append(f"\n---")
        lines.append(f"\n### 📋 데이터 안내")
        lines.append(f"- **정산 데이터**: {date} 기준 (청과법인 {len(corp_data)}개사)")
        lines.append(f"- **대전 비교 데이터**: {compare_date} 기준 (공판장 포함)")
        lines.append(f"- **공판장 정산 지연 안내**: 농협/원협 공판장은 청과법인보다 정산 데이터 업로드가 1~2일 늦습니다.")
        lines.append(f"  - 청과법인(예: 대전중앙청과㈜, 대전청과㈜): 당일 정산 즉시 반영")
        lines.append(f"  - 공판장(예: 농협대전(공), 대전원협노은(공)): 1~2일 후 반영")
        lines.append(f"  - 따라서 대전 지역 전체 비교는 공판장 데이터가 포함된 **{compare_date}** 기준을 사용합니다.")
        lines.append(f"  - 매일 D-1, D-2 데이터를 자동 재수집하여 공판장 정산이 반영되면 즉시 업데이트됩니다.")

    lines.append(f"\n*자동 생성 by 송봇 | data.go.kr 정산 데이터 기준*")

    return "\n".join(lines)


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

    print("\n" + report)


if __name__ == "__main__":
    main()
