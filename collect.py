"""
도매시장 일일 정산 데이터 수집기
data.go.kr API → JSON 저장

정산일(trd_clcln_ymd) 기준 = 경매 후 취소 건 제외된 확정 거래
경매 낙찰 시간(scsbd_dt)과 정산일이 다를 수 있음

사용: python collect.py [--date 2026-03-09] [--markets 250003,110001]
기본: 오늘 날짜, 전국 주요 12개 시장
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

API_BASE = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

# 전국 주요 12개 시장
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

PAGE_SIZE = 1000


def fetch_page(date: str, market_code: str, page: int = 1,
               num_rows: int = PAGE_SIZE) -> tuple[list[dict], int]:
    """data.go.kr 경매 데이터 1페이지 조회. (items, totalCount) 반환"""
    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "pageNo": str(page),
        "numOfRows": str(num_rows),
        "cond[trd_clcln_ymd::EQ]": date,
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
        return [], total
    except Exception as e:
        print(f"    Error: {e}")
        return [], 0


def fetch_all(date: str, market_code: str) -> tuple[list[dict], int]:
    """시장 전체 데이터 페이지네이션으로 수집"""
    first_items, total = fetch_page(date, market_code, page=1)
    if total == 0:
        return [], 0

    all_items = first_items
    collected = len(first_items)

    # 전체 건수 다 가져오기
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
    price = float(item.get("scsbd_prc", 0))
    qty = float(item.get("qty", 0))
    unit_qty = float(item.get("unit_qty", 0))
    return {
        "auction_time": item.get("scsbd_dt", ""),       # 경매 낙찰 시간
        "settle_date": item.get("trd_clcln_ymd", ""),    # 정산일 (확정)
        "modified_at": item.get("mdfcn_dt", ""),         # 수정 시간
        "market_name": item.get("whsl_mrkt_nm", ""),
        "market_code": item.get("whsl_mrkt_cd", ""),
        "corp_name": item.get("corp_nm", ""),            # 법인 (경매사)
        "trade_type": item.get("trd_se", ""),            # 경매/정가매매
        "category": item.get("gds_lclsf_nm", ""),       # 대분류
        "category_code": item.get("gds_lclsf_cd", ""),
        "product": item.get("gds_mclsf_nm", ""),        # 품목 (사과, 배 등)
        "variety": item.get("gds_sclsf_nm", ""),        # 품종 (후지, 홍로 등)
        "price": int(price),                             # 낙찰 단가 (원)
        "quantity": int(qty) if qty == int(qty) else qty,
        "unit": item.get("unit_nm", ""),                 # 단위 (kg, 개 등)
        "unit_weight": unit_qty,                         # 단위 중량
        "packaging": item.get("pkg_nm", ""),             # 포장 (상자, 봉지 등)
        "origin": item.get("plor_nm", "").strip(),       # 산지
    }


def collect(date: str, market_codes: dict[str, str]) -> dict:
    """전체 수집 실행"""
    print(f"수집 시작: {date} (정산일 기준)")
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
        "data_type": "settlement",  # 정산 데이터
        "collected_at": datetime.now().isoformat(),
        "total_available": total_available,
        "total_collected": total_count,
        "market_count": len(market_codes),
        "markets": all_data,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"auction_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n총 {total_count:,}건 수집 (전국 {total_available:,}건 중) → {out_file}")
    return result


def main():
    if not API_KEY:
        print("ERROR: DATA_GO_KR_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="도매시장 일일 정산 데이터 수집")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="정산 날짜 (YYYY-MM-DD)")
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

    collect(args.date, markets)


if __name__ == "__main__":
    main()
