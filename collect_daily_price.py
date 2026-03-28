"""
일별 도소매 가격 수집기
data.go.kr perDay/price API → JSON 저장

출처: KAMIS
일별 도매/소매 조사가격 + kg환산가격

사용: python collect_daily_price.py [--date 2026-03-27] [--category 200]
기본: 어제 날짜, 전체 부류
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

API_BASE = "https://apis.data.go.kr/B552845/perDay/price"
API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

OUTPUT_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

PAGE_SIZE = 1000

# 주요 부류 코드 (KAMIS 기준)
CATEGORIES = {
    "100": "식량작물",
    "200": "채소류",
    "300": "특용작물",
    "400": "과일류",
    "500": "축산물",
    "600": "수산물",
}


def fetch_page(date_yyyymmdd: str, category_code: str,
               page: int = 1) -> tuple[list[dict], int]:
    """일별 도소매 1페이지 조회. (items, totalCount) 반환"""
    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "pageNo": str(page),
        "numOfRows": str(PAGE_SIZE),
        "cond[exmn_ymd::GTE]": date_yyyymmdd,
        "cond[exmn_ymd::LTE]": date_yyyymmdd,
        "cond[ctgry_cd::EQ]": category_code,
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


def fetch_all_for_category(date_yyyymmdd: str,
                           category_code: str) -> tuple[list[dict], int]:
    """부류별 전체 데이터 수집"""
    first_items, total = fetch_page(date_yyyymmdd, category_code, page=1)
    if total <= 0:
        return first_items, total

    all_items = first_items
    collected = len(first_items)

    page = 2
    while collected < total:
        print(f"      페이지 {page} ({collected:,}/{total:,}건)...")
        items, t = fetch_page(date_yyyymmdd, category_code, page=page)
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
        "se_code": item.get("se_cd", ""),
        "se_name": item.get("se_nm", ""),                # 구분 (도매/소매)
        "category_code": item.get("ctgry_cd", ""),
        "category_name": item.get("ctgry_nm", ""),       # 부류
        "item_code": item.get("item_cd", ""),
        "item_name": item.get("item_nm", ""),            # 품목
        "variety_code": item.get("vrty_cd", ""),
        "variety_name": item.get("vrty_nm", ""),         # 품종
        "grade_code": item.get("grd_cd", ""),
        "grade_name": item.get("grd_nm", ""),            # 등급
        "unit": item.get("unit", ""),
        "unit_size": item.get("unit_sz", ""),
        "market_name": item.get("mrkt_nm", ""),          # 시장/지역
        "avg_price": safe_float(item.get("exmn_dd_avg_prc")),
        "avg_price_kg": safe_float(item.get("exmn_dd_cnvs_avg_prc")),
    }


def collect_daily_price(date: str, categories: dict[str, str]) -> dict:
    """일별 도소매 전체 수집"""
    date_yyyymmdd = date.replace("-", "")
    print(f"일별 도소매 수집 시작: {date} ({date_yyyymmdd})")
    print(f"부류: {len(categories)}개")

    all_formatted = []
    category_counts = {}

    for code, name in categories.items():
        print(f"\n  [{name}] 수집 중...")
        items, total = fetch_all_for_category(date_yyyymmdd, code)
        if total == -1:
            print(f"  [{name}] API 한도 초과, 중단")
            break
        formatted = [format_item(i) for i in items]
        all_formatted.extend(formatted)
        category_counts[name] = len(formatted)
        print(f"  [{name}] {len(formatted):,}건")

    if not all_formatted:
        print(f"데이터 없음 (휴장일?)")
        return {}

    result = {
        "date": date,
        "data_type": "daily_price",
        "collected_at": datetime.now().isoformat(),
        "total_count": len(all_formatted),
        "category_counts": category_counts,
        "items": all_formatted,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"daily_price_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 아카이브 저장
    month_dir = ARCHIVE_DIR / date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    archive_file = month_dir / f"daily_price_{date}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(all_formatted):,}건 수집")
    print(f"부류별: {', '.join(f'{k}({v})' for k, v in category_counts.items())}")
    print(f"저장: {out_file}")
    print(f"아카이브: {archive_file}")
    return result


def main():
    if not API_KEY:
        print("ERROR: DATA_GO_KR_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="일별 도소매 가격 수집")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=yesterday,
                        help="조사일자 (YYYY-MM-DD, 기본: 어제)")
    parser.add_argument("--category", default="",
                        help="부류 코드 (콤마 구분, 예: 200,400. 기본: 전체)")
    args = parser.parse_args()

    if args.category:
        codes = [c.strip() for c in args.category.split(",")]
        cats = {c: CATEGORIES.get(c, f"부류_{c}") for c in codes}
    else:
        cats = CATEGORIES

    collect_daily_price(args.date, cats)


if __name__ == "__main__":
    main()
