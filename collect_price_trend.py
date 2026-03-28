"""
도매시장 가격 추이 수집기
data.go.kr priceSequel/info API → JSON 저장

출처: KAMIS 중도매인 조사
품목별 조사가격 + kg환산가격 + 1~4주 전 비교 데이터

사용: python collect_price_trend.py [--date 2026-03-27]
기본: 어제 날짜
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

API_BASE = "https://apis.data.go.kr/B552845/priceSequel/info"
API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

OUTPUT_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

PAGE_SIZE = 1000


def fetch_page(date_yyyymmdd: str, page: int = 1) -> tuple[list[dict], int]:
    """가격 추이 1페이지 조회. (items, totalCount) 반환"""
    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "pageNo": str(page),
        "numOfRows": str(PAGE_SIZE),
        "cond[exmn_ymd::EQ]": date_yyyymmdd,
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
    """전체 가격 추이 데이터 수집"""
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
        if t == -1:
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
        "exam_date": item.get("exmn_ymd", ""),
        "category": item.get("se_nm", ""),            # 구분 (도매/소매/중도매)
        "category_code": item.get("ctgry_cd", ""),
        "category_name": item.get("ctgry_nm", ""),    # 부류
        "item_code": item.get("item_cd", ""),
        "item_name": item.get("item_nm", ""),          # 품목
        "variety_code": item.get("vrty_cd", ""),
        "variety_name": item.get("vrty_nm", ""),       # 품종
        "grade_code": item.get("grd_cd", ""),
        "grade_name": item.get("grd_nm", ""),          # 등급
        "unit": item.get("unit_nm", ""),
        "unit_code": item.get("unit_cd", ""),
        "avg_price": safe_float(item.get("exmn_dd_avg_prc")),
        "avg_price_kg": safe_float(item.get("exmn_dd_cnvs_avg_prc")),
        "w1_avg_price": safe_float(item.get("ww1_bfr_avg_prc")),
        "w1_avg_price_kg": safe_float(item.get("ww1_bfr_cnvs_avg_prc")),
        "w2_avg_price": safe_float(item.get("ww2_bfr_avg_prc")),
        "w2_avg_price_kg": safe_float(item.get("ww2_bfr_cnvs_avg_prc")),
        "w3_avg_price": safe_float(item.get("ww3_bfr_avg_prc")),
        "w3_avg_price_kg": safe_float(item.get("ww3_bfr_cnvs_avg_prc")),
        "w4_avg_price": safe_float(item.get("ww4_bfr_avg_prc")),
        "w4_avg_price_kg": safe_float(item.get("ww4_bfr_cnvs_avg_prc")),
    }


def collect_price_trend(date: str) -> dict:
    """가격 추이 전체 수집"""
    date_yyyymmdd = date.replace("-", "")
    print(f"가격 추이 수집 시작: {date} ({date_yyyymmdd})")

    all_items, total = fetch_all(date_yyyymmdd)
    if total == -1:
        print("API 한도 초과로 중단")
        return {}
    if total == 0:
        print(f"데이터 없음 (휴장일?)")
        return {}

    formatted = [format_item(i) for i in all_items]

    # 구분별 건수 요약
    category_summary = {}
    for item in formatted:
        cat = item["category"] or "기타"
        category_summary[cat] = category_summary.get(cat, 0) + 1

    result = {
        "date": date,
        "data_type": "price_trend",
        "collected_at": datetime.now().isoformat(),
        "total_count": total,
        "collected_count": len(formatted),
        "category_summary": category_summary,
        "items": formatted,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"price_trend_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 아카이브 저장
    month_dir = ARCHIVE_DIR / date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    archive_file = month_dir / f"price_trend_{date}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(formatted):,}건 수집 (API 전체 {total:,}건)")
    print(f"구분별: {', '.join(f'{k}({v})' for k, v in sorted(category_summary.items(), key=lambda x: -x[1]))}")
    print(f"저장: {out_file}")
    print(f"아카이브: {archive_file}")
    return result


def main():
    if not API_KEY:
        print("ERROR: DATA_GO_KR_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="도매시장 가격 추이 수집")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=yesterday,
                        help="조사일자 (YYYY-MM-DD, 기본: 어제)")
    args = parser.parse_args()

    collect_price_trend(args.date)


if __name__ == "__main__":
    main()
