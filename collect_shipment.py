"""
전자송품장 출하예약정보 수집기
data.go.kr 전자송품장 API → JSON 저장

출하예정일(spmt_dt) 기준 = 경매 전 사전 등록된 출하예약 정보
오늘 수집 시 내일(+1일) 출하예정 물량을 가져옴 → 수급 예측에 활용

사용: python collect_shipment.py [--date 2026-03-20] [--markets 250003,110001]
기본: 내일 날짜, 전국 주요 12개 시장
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

API_BASE = "https://apis.data.go.kr/B552845/katElectronicInvoice2/shipmentReservations2"
API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

# 전국 주요 12개 시장 (collect.py와 동일)
DEFAULT_MARKETS = {
    "110001": "서울가락",
    "110008": "서울강서",
    "230001": "인천남촌",
    "230003": "인천삼산",
    "250001": "대전오정",
    "250003": "대전노은",
    "220001": "대구북부",
    "210001": "부산엄궁",
    "240001": "광주각화",
    "240004": "광주서부",
    "350101": "전주",
    "380101": "창원팔용",
}

OUTPUT_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

PAGE_SIZE = 1000


def fetch_page(date: str, market_code: str, page: int = 1,
               num_rows: int = PAGE_SIZE) -> tuple[list[dict], int]:
    """전자송품장 출하예약 1페이지 조회. (items, totalCount) 반환"""
    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "pageNo": str(page),
        "numOfRows": str(num_rows),
        "cond[spmt_dt::EQ]": date,
        "cond[whsl_mrkt_cd::EQ]": market_code,
    }

    try:
        resp = httpx.get(API_BASE, params=params, timeout=30.0)
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


def fetch_all(date: str, market_code: str) -> tuple[list[dict], int]:
    """시장 전체 출하예약 데이터 페이지네이션으로 수집"""
    first_items, total = fetch_page(date, market_code, page=1)
    if total == 0:
        return [], 0

    all_items = first_items
    collected = len(first_items)

    page = 2
    while collected < total:
        items, _ = fetch_page(date, market_code, page=page)
        if not items:
            break
        all_items.extend(items)
        collected += len(items)
        page += 1

    return all_items, total


def format_item(item: dict) -> dict:
    """API 응답 → 정리된 딕셔너리"""
    unit_qty = float(item.get("unit_qty", 0) or 0)
    spmt_qty = float(item.get("spmt_qty", 0) or 0)
    return {
        "einvc_no": item.get("einvc_no", ""),           # 전자송장번호
        "shipment_date": item.get("spmt_dt", ""),        # 출하예정일
        "market_name": item.get("whsl_mrkt_nm", ""),
        "market_code": item.get("whsl_mrkt_cd", ""),
        "corp_name": item.get("corp_nm", ""),            # 법인
        "corp_code": item.get("corp_cd", ""),
        "trade_type": item.get("trd_se", ""),            # 매매구분
        "category": item.get("gds_lclsf_nm", ""),       # 대분류
        "category_code": item.get("gds_lclsf_cd", ""),
        "product": item.get("gds_mclsf_nm", ""),        # 품목
        "product_code": item.get("gds_mclsf_cd", ""),
        "variety": item.get("gds_sclsf_nm", ""),        # 품종
        "variety_code": item.get("gds_sclsf_cd", ""),
        "quantity": int(spmt_qty) if spmt_qty == int(spmt_qty) else spmt_qty,
        "unit": item.get("unit_nm", ""),                 # 단위
        "unit_weight": unit_qty,                         # 단위 중량
        "packaging": item.get("pkg_nm", ""),             # 포장
        "grade": item.get("grd_nm", ""),                 # 등급
        "status": item.get("einvc_stts", ""),            # 송품장 상태
    }


def collect_shipment(date: str, market_codes: dict[str, str]) -> dict:
    """전자송품장 출하예약 전체 수집"""
    print(f"출하예약 수집 시작: {date} (출하예정일 기준)")
    print(f"대상 시장: {len(market_codes)}개")

    all_data = {}
    total_count = 0
    total_available = 0

    for code, name in market_codes.items():
        print(f"\n  [{name}] 수집 중...")
        items, available = fetch_all(date, code)
        formatted = [format_item(i) for i in items]
        all_data[code] = {
            "market_name": name,
            "total_available": available,
            "collected": len(formatted),
            "items": formatted,
        }
        total_count += len(formatted)
        total_available += available
        if available > len(formatted):
            print(f"  [{name}] {len(formatted):,}건 수집 (전체 {available:,}건)")
        else:
            print(f"  [{name}] {len(formatted):,}건")

    result = {
        "date": date,
        "data_type": "shipment_reservation",  # 출하예약
        "collected_at": datetime.now().isoformat(),
        "total_available": total_available,
        "total_collected": total_count,
        "market_count": len(market_codes),
        "markets": all_data,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"shipment_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 아카이브 저장
    month_dir = ARCHIVE_DIR / date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    archive_file = month_dir / f"shipment_{date}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n총 {total_count:,}건 출하예약 수집 (전국 {total_available:,}건 중) → {out_file}")
    print(f"아카이브: {archive_file}")
    return result


def main():
    if not API_KEY:
        print("ERROR: DATA_GO_KR_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="전자송품장 출하예약정보 수집")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=tomorrow,
                        help="출하예정일 (YYYY-MM-DD, 기본: 내일)")
    parser.add_argument("--markets", default="",
                        help="시장 코드 (콤마 구분, 예: 250003,110001)")
    args = parser.parse_args()

    if args.markets:
        codes = args.markets.split(",")
        markets = {}
        for c in codes:
            c = c.strip()
            markets[c] = DEFAULT_MARKETS.get(c, f"시장_{c}")
    else:
        markets = DEFAULT_MARKETS

    collect_shipment(args.date, markets)


if __name__ == "__main__":
    main()
