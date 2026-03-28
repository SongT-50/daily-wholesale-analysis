"""
도매시장 출하량 추이 수집기
data.go.kr shipmentSequel/info API → JSON 저장

출처: 도매시장 실데이터 (전자송품장 아님!)
법인별·품목별 출하수량/금액 + 1~4주 전 비교 데이터

사용: python collect_shipment_trend.py [--date 2026-03-28]
기본: 어제 날짜 (정산 데이터는 당일 미반영)
"""

import sys
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv(Path(__file__).parent.parent / ".env")

API_BASE = "https://apis.data.go.kr/B552845/shipmentSequel/info"
API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

OUTPUT_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

PAGE_SIZE = 1000


def fetch_page(date_yyyymmdd: str, page: int = 1) -> tuple[list[dict], int]:
    """출하량 추이 1페이지 조회. (items, totalCount) 반환"""
    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "pageNo": str(page),
        "numOfRows": str(PAGE_SIZE),
        "cond[spmt_ymd::EQ]": date_yyyymmdd,
    }

    try:
        resp = httpx.get(API_BASE, params=params, timeout=30.0)
        if resp.status_code == 429:
            print(f"    API 한도 초과 (429)")
            return [], -1
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}")
            return [], 0
        data = resp.json()
        body = data.get("response", {}).get("body", {})
        total = body.get("totalCount", 0)
        items = body.get("items", {}).get("item", [])
        if isinstance(items, list):
            return items, total
        if isinstance(items, dict):
            return [items], total
        return [], total
    except Exception as e:
        print(f"    Error: {e}")
        return [], 0


def fetch_all(date_yyyymmdd: str) -> tuple[list[dict], int]:
    """전체 출하량 추이 데이터 페이지네이션으로 수집"""
    first_items, total = fetch_page(date_yyyymmdd, page=1)
    if total <= 0:
        return first_items, total

    all_items = first_items
    collected = len(first_items)

    page = 2
    while collected < total:
        print(f"    페이지 {page} ({collected:,}/{total:,}건)...")
        items, t = fetch_page(date_yyyymmdd, page=page)
        if not items:
            break
        if t == -1:  # 429
            break
        all_items.extend(items)
        collected += len(items)
        page += 1

    return all_items, total


def format_item(item: dict) -> dict:
    """API 응답 → 정리된 딕셔너리"""
    def safe_float(v):
        try:
            return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    return {
        "shipment_date": item.get("spmt_ymd", ""),
        "market_code": item.get("whsl_mrkt_cd", ""),
        "market_name": item.get("whsl_mrkt_nm", ""),
        "corp_code": item.get("corp_cd", ""),
        "corp_name": item.get("corp_nm", ""),
        "category_l": item.get("gds_lclsf_nm", ""),       # 대분류
        "category_l_code": item.get("gds_lclsf_cd", ""),
        "category_m": item.get("gds_mclsf_nm", ""),       # 중분류
        "category_m_code": item.get("gds_mclsf_cd", ""),
        "category_s": item.get("gds_sclsf_nm", ""),       # 소분류
        "category_s_code": item.get("gds_sclsf_cd", ""),
        "avg_shipment_qty": safe_float(item.get("avg_spmt_qty")),
        "avg_shipment_amt": safe_float(item.get("avg_spmt_amt")),
        "w1_avg_qty": safe_float(item.get("ww1_bfr_avg_spmt_qty")),
        "w1_avg_amt": safe_float(item.get("ww1_bfr_avg_spmt_amt")),
        "w2_avg_qty": safe_float(item.get("ww2_bfr_avg_spmt_qty")),
        "w2_avg_amt": safe_float(item.get("ww2_bfr_avg_spmt_amt")),
        "w3_avg_qty": safe_float(item.get("ww3_bfr_avg_spmt_qty")),
        "w3_avg_amt": safe_float(item.get("ww3_bfr_avg_spmt_amt")),
        "w4_avg_qty": safe_float(item.get("ww4_bfr_avg_spmt_qty")),
        "w4_avg_amt": safe_float(item.get("ww4_bfr_avg_spmt_amt")),
    }


def collect_shipment_trend(date: str) -> dict:
    """출하량 추이 전체 수집"""
    date_yyyymmdd = date.replace("-", "")
    print(f"출하량 추이 수집 시작: {date} ({date_yyyymmdd})")

    all_items, total = fetch_all(date_yyyymmdd)
    if total == -1:
        print("API 한도 초과로 중단")
        return {}
    if total == 0:
        print(f"데이터 없음 (휴장일?)")
        return {}

    formatted = [format_item(i) for i in all_items]

    # 시장별 건수 요약
    market_summary = {}
    for item in formatted:
        mk = item["market_name"] or item["market_code"]
        market_summary[mk] = market_summary.get(mk, 0) + 1

    result = {
        "date": date,
        "data_type": "shipment_trend",
        "collected_at": datetime.now().isoformat(),
        "total_count": total,
        "collected_count": len(formatted),
        "market_summary": market_summary,
        "items": formatted,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"shipment_trend_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 아카이브 저장
    month_dir = ARCHIVE_DIR / date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    archive_file = month_dir / f"shipment_trend_{date}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(formatted):,}건 수집 (API 전체 {total:,}건)")
    print(f"시장별: {', '.join(f'{k}({v})' for k, v in sorted(market_summary.items(), key=lambda x: -x[1])[:10])}...")
    print(f"저장: {out_file}")
    print(f"아카이브: {archive_file}")
    return result


def main():
    if not API_KEY:
        print("ERROR: DATA_GO_KR_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="도매시장 출하량 추이 수집")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=yesterday,
                        help="출하일자 (YYYY-MM-DD, 기본: 어제)")
    args = parser.parse_args()

    collect_shipment_trend(args.date)


if __name__ == "__main__":
    main()
