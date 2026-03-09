"""
도매시장 일일 분석 리포트 생성기
수집 데이터(JSON) → Gemini AI 분석 → 마크다운 리포트

사용: python analyze.py [--date 2026-03-09]
"""

import sys
import os
import json
import argparse
from datetime import datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv(Path(__file__).parent.parent / ".env")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DATA_DIR = Path(__file__).parent / "data"
REPORT_DIR = Path(__file__).parent / "reports"


def load_auction_data(date: str) -> dict | None:
    """수집된 경매 데이터 로드"""
    data_file = DATA_DIR / f"auction_{date}.json"
    if not data_file.exists():
        print(f"데이터 파일 없음: {data_file}")
        return None
    with open(data_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _filter_outliers(prices: list[int | float]) -> list[int | float]:
    """이상치 제거 — 절대 상한 + 중앙값 기준 5배 초과 or 1/5 미만 제외"""
    # 절대 상한: kg당 100,000원 초과는 입력 오류 (팔레트 단위 등)
    prices = [p for p in prices if p <= 100_000]
    if len(prices) < 3:
        return prices
    sorted_p = sorted(prices)
    median = sorted_p[len(sorted_p) // 2]
    if median == 0:
        return prices
    return [p for p in prices if median / 5 <= p <= median * 5]


def summarize_data(data: dict) -> str:
    """Gemini에 보낼 요약 텍스트 생성 (토큰 절약)"""
    date = data["date"]
    lines = [f"# 도매시장 경매 데이터 요약 ({date})\n"]
    total = data.get("total_collected", data.get("total_count", 0))
    available = data.get("total_available", total)
    lines.append(f"총 거래: {total:,}건 (전국 {available:,}건 중 수집)\n")

    for code, market in data["markets"].items():
        name = market["market_name"]
        items = market["items"]
        if not items:
            continue

        lines.append(f"\n## {name} ({len(items)}건)")

        # 품목별 집계 (kg당 단가 기준)
        product_stats: dict[str, dict] = {}
        for item in items:
            product = item["product"]
            unit_wt = item.get("unit_weight", 0)
            price = item.get("price", 0)
            # kg당 단가 환산
            per_kg = price / unit_wt if unit_wt > 0 else 0
            if per_kg <= 0:
                continue
            if product not in product_stats:
                product_stats[product] = {
                    "count": 0,
                    "per_kg_prices": [],
                    "total_qty": 0,
                    "main_unit": unit_wt,
                }
            product_stats[product]["count"] += 1
            product_stats[product]["per_kg_prices"].append(per_kg)
            product_stats[product]["total_qty"] += (
                item["quantity"] if isinstance(item["quantity"], (int, float)) else 0
            )

        # 상위 10개 품목
        top = sorted(product_stats.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
        for product, stats in top:
            prices = _filter_outliers(stats["per_kg_prices"])
            avg_price = sum(prices) / len(prices) if prices else 0
            min_price = min(prices) if prices else 0
            max_price = max(prices) if prices else 0
            lines.append(
                f"  - {product}: {stats['count']}건, "
                f"평균 {avg_price:,.0f}원/kg, "
                f"범위 {min_price:,.0f}~{max_price:,.0f}원/kg, "
                f"총수량 {stats['total_qty']}건"
            )

    return "\n".join(lines)


def analyze_with_gemini(summary: str, date: str) -> str:
    """Gemini API로 분석 리포트 생성"""
    if not GEMINI_API_KEY:
        return _fallback_report(summary, date)

    prompt = f"""당신은 한국 농산물 도매시장 전문 분석가입니다.
아래 경매 데이터를 분석해서 실용적인 일일 리포트를 작성하세요.

## 분석 요청
- 날짜: {date}
- 대상: 전국 도매시장 경매 데이터 (12개 시장, 38개 법인)

## 중요 용어
- 정산일(trd_clcln_ymd): 경매 후 취소건 제외, 실제 확정된 거래 날짜
- 단가(scsbd_prc): 낙찰 가격 (단위중량당 원)
- 법인(corp_nm): 경매를 진행한 도매법인 (중간유통)

## 리포트 구성
1. **오늘의 핵심 요약** (3줄 이내)
2. **주요 품목 시세** (가격 동향, 특이사항, 전일대비 가능하면)
3. **시장별 특징** (거래량 많은 시장, 특이 동향)
4. **도매법인 동향** (활발한 법인, 특이사항)
5. **주목할 점** (계절 요인, 수급 변화 등)
6. **내일 전망** (간단히)

## 규칙
- 한국어로 작성, 도매시장 업계 용어 사용
- 숫자는 콤마 포함 (예: 15,000원/10kg)
- 추측이면 "~로 보입니다" 표현 사용
- 짧고 실용적으로 — 새벽에 읽을 실무자 대상

## 데이터
{summary}
"""

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
        resp = httpx.post(
            url,
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 2000,
                },
            },
            timeout=60.0,
        )
        if resp.status_code != 200:
            print(f"Gemini API 오류: {resp.status_code}")
            return _fallback_report(summary, date)

        result = resp.json()
        text = result["candidates"][0]["content"]["parts"][0]["text"]
        return text
    except Exception as e:
        print(f"Gemini 분석 실패: {e}")
        return _fallback_report(summary, date)


def _fallback_report(summary: str, date: str) -> str:
    """Gemini 실패 시 기본 리포트"""
    return f"""# 도매시장 일일 리포트 ({date})

> Gemini API를 사용할 수 없어 원본 데이터 요약만 제공합니다.

{summary}
"""


def _djc_report(data: dict, date: str) -> str:
    """대전노은 대전중앙청과㈜ 전용 분석"""
    from collections import Counter, defaultdict

    items = []
    for m in data["markets"].values():
        for item in m["items"]:
            if item.get("market_name") == "대전노은" and "대전중앙청과" in item.get("corp_name", ""):
                items.append(item)

    if not items:
        return ""

    lines = [f"\n---\n## 📌 대전노은 대전중앙청과㈜ 상세 ({len(items):,}건)\n"]

    # 품목별 집계
    product_stats: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "prices": [], "qty": 0, "origins": Counter()}
    )
    for item in items:
        product = item["product"]
        unit_wt = item.get("unit_weight", 0)
        price = item.get("price", 0)
        per_kg = price / unit_wt if unit_wt > 0 else 0
        qty = item["quantity"] if isinstance(item["quantity"], (int, float)) else 0

        product_stats[product]["count"] += 1
        product_stats[product]["qty"] += qty
        if per_kg > 0:
            product_stats[product]["prices"].append(per_kg)
        origin = item.get("origin", "-")
        if origin and origin != "-":
            product_stats[product]["origins"][origin] += 1

    # 거래건수 상위 품목
    top_products = sorted(product_stats.items(), key=lambda x: x[1]["count"], reverse=True)

    lines.append("### 품목별 거래 현황\n")
    lines.append("| 순위 | 품목 | 건수 | 총수량 | 평균(원/kg) | 범위 |")
    lines.append("|------|------|------|--------|-----------|------|")
    for i, (product, s) in enumerate(top_products[:20], 1):
        prices = _filter_outliers(s["prices"])
        if not prices:
            continue
        avg = sum(prices) / len(prices)
        mn = min(prices)
        mx = max(prices)
        lines.append(
            f"| {i} | {product} | {s['count']:,} | {s['qty']:,} | "
            f"{avg:,.0f} | {mn:,.0f}~{mx:,.0f} |"
        )

    # 주요 품목 산지 분포
    lines.append("\n### 주요 품목 산지 분포\n")
    for product, s in top_products[:10]:
        if not s["origins"]:
            continue
        lines.append(f"**{product}** ({s['count']:,}건)")
        top_origins = sorted(s["origins"].items(), key=lambda x: x[1], reverse=True)
        for origin, cnt in top_origins[:5]:
            pct = cnt / s["count"] * 100
            lines.append(f"- {origin}: {cnt}건 ({pct:.0f}%)")
        lines.append("")

    # 전체 산지 분포
    all_origins = Counter()
    for item in items:
        origin = item.get("origin", "-")
        if origin and origin != "-":
            all_origins[origin] += 1

    lines.append("### 전체 산지 분포 (TOP 15)\n")
    lines.append("| 순위 | 산지 | 건수 | 비중 |")
    lines.append("|------|------|------|------|")
    for i, (origin, cnt) in enumerate(all_origins.most_common(15), 1):
        pct = cnt / len(items) * 100
        lines.append(f"| {i} | {origin} | {cnt:,} | {pct:.1f}% |")

    return "\n".join(lines)


def generate_report(date: str) -> str | None:
    """전체 분석 파이프라인"""
    data = load_auction_data(date)
    if not data:
        return None

    total = data.get("total_collected", data.get("total_count", 0))
    print(f"분석 시작: {date} ({total:,}건)")

    # 법인 수 계산
    corps = set()
    for m in data["markets"].values():
        for item in m["items"]:
            corps.add(f"{item['market_name']}|{item['corp_name']}")
    corp_count = len(corps)

    # 요약 생성
    summary = summarize_data(data)
    print(f"요약 생성 완료 ({len(summary)} chars)")

    # AI 분석
    print("Gemini 분석 중...")
    report = analyze_with_gemini(summary, date)

    # 대전중앙청과 전용 섹션 추가
    djc = _djc_report(data, date)
    if djc:
        report += djc
        print("대전중앙청과㈜ 상세 섹션 추가")

    # 저장
    REPORT_DIR.mkdir(exist_ok=True)
    report_file = REPORT_DIR / f"report_{date}.md"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"리포트 저장: {report_file} (전국 {corp_count}개 법인)")
    return report


def main():
    parser = argparse.ArgumentParser(description="도매시장 일일 분석 리포트 생성")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="분석 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    report = generate_report(args.date)
    if report:
        print("\n" + "=" * 60)
        print(report[:500] + "..." if len(report) > 500 else report)


if __name__ == "__main__":
    main()
