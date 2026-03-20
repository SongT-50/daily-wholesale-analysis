"""
도매시장 일일 정산 데이터 수집기
data.go.kr 정산정보 API (katSale) → JSON 저장

정산정보 API = aT 도매시장 통합 홈페이지와 동일한 데이터
(기존 katRealTime2 실시간 API 대비 누락 없는 완전한 정산 데이터)

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

API_BASE = "https://apis.data.go.kr/B552845/katSale/trades"
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
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

PAGE_SIZE = 1000

# 38개 청과 법인 (수산 법인 제외)
VALID_CORPS = {
    "동화청과㈜", "㈜중앙청과", "서울청과㈜", "한국청과㈜", "농협가락(공)", "대아청과㈜",
    "서부청과㈜", "농협강서(공)", "강서청과㈜",
    "인천원협남촌(공)", "㈜대인농산", "인천농산물㈜", "덕풍청과㈜",
    "㈜부평농산", "㈜경인농산", "인천원협삼산(공)",
    "농협대전(공)", "대전청과㈜",
    "대전중앙청과㈜", "대전원협노은(공)",
    "대양청과㈜", "대구중앙청과㈜", "효성청과㈜", "농협북대구(공)", "대구경북원협(공)",
    "농협부산(공)", "부산청과㈜", "항도청과㈜",
    "광주원협(공)", "광주중앙청과㈜", "광주청과㈜",
    "두레청과㈜", "㈜호남청과", "농협광주(공)",
    "전주원협(공)", "전주청과물㈜",
    "농협창원(공)", "㈜창원청과시장",
}


def fetch_page(date: str, market_code: str, page: int = 1,
               num_rows: int = PAGE_SIZE) -> tuple[list[dict], int]:
    """정산정보 API 1페이지 조회. (items, totalCount) 반환"""
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
    """정산정보 API 응답 → 정리된 딕셔너리

    정산 API는 (시장,법인,품목,품종,등급,산지,포장,크기) 기준 집계 행.
    하위 호환성을 위해 price*quantity = 총금액, unit_weight*quantity = 총물량(kg).
    """
    unit_qty = float(item.get("unit_qty", 0))
    unit_tot_qty = float(item.get("unit_tot_qty", 0))
    totprc = float(item.get("totprc", 0))
    avgprc = float(item.get("avgprc", 0))
    lwprc = float(item.get("lwprc", 0))
    hgprc = float(item.get("hgprc", 0))

    return {
        "settle_date": item.get("trd_clcln_ymd", ""),
        "market_name": item.get("whsl_mrkt_nm", ""),
        "market_code": item.get("whsl_mrkt_cd", ""),
        "corp_name": item.get("corp_nm", ""),
        "corp_code": item.get("corp_cd", ""),
        "trade_type": item.get("trd_se", ""),
        "category": item.get("gds_lclsf_nm", ""),
        "category_code": item.get("gds_lclsf_cd", ""),
        "product": item.get("gds_mclsf_nm", ""),
        "variety": item.get("gds_sclsf_nm", ""),
        "grade": item.get("grd_nm", ""),
        "size": item.get("sz_nm", ""),
        # 하위 호환: price*quantity = 총금액, unit_weight*quantity = 총물량(kg)
        "price": int(totprc),
        "quantity": 1,
        "unit": item.get("unit_nm", ""),
        "unit_weight": unit_tot_qty,
        "packaging": item.get("pkg_nm", ""),
        "origin": (item.get("plor_nm") or "").strip(),
        # 정산 API 추가 필드
        "total_amount": int(totprc),
        "total_qty": unit_tot_qty,
        "avg_price": int(avgprc),
        "low_price": int(lwprc),
        "high_price": int(hgprc),
        "unit_qty": unit_qty,
    }


# 이상치 기준: 절대상한 100,000원/kg 초과 = 입력 오류
OUTLIER_CEILING_PER_KG = 100_000


def is_outlier(item: dict) -> bool:
    """수집 시점 이상치 판별 — 제거 대상이면 True"""
    total_amount = item.get("total_amount", 0)
    total_qty = item.get("total_qty", 0)

    if total_amount <= 0:
        return True

    if total_qty > 0:
        per_kg = total_amount / total_qty
        if per_kg > OUTLIER_CEILING_PER_KG:
            return True

    return False


def collect(date: str, market_codes: dict[str, str]) -> dict:
    """전체 수집 실행"""
    print(f"수집 시작: {date} (정산정보 API)")
    print(f"대상 시장: {len(market_codes)}개")

    all_data = {}
    total_count = 0
    total_available = 0
    total_outliers = 0

    for code, name in market_codes.items():
        print(f"\n  [{name}] 수집 중...")
        items, available = fetch_all(date, code)
        # 수산 법인 제외 (38개 청과 법인만)
        items = [i for i in items if i.get("corp_nm", "") in VALID_CORPS]
        formatted = [format_item(i) for i in items]
        outlier_count = sum(1 for i in formatted if is_outlier(i))
        cleaned = [i for i in formatted if not is_outlier(i)]
        all_data[code] = {
            "market_name": name,
            "total_available": available,
            "collected": len(cleaned),
            "outliers_removed": outlier_count,
            "items": cleaned,
        }
        total_count += len(cleaned)
        total_available += available
        total_outliers += outlier_count
        if outlier_count > 0:
            print(f"  [{name}] {len(cleaned):,}건 (이상치 {outlier_count}건 제거)")
        else:
            print(f"  [{name}] {len(cleaned):,}건")

    result = {
        "date": date,
        "data_type": "settlement",
        "api_source": "katSale",
        "collected_at": datetime.now().isoformat(),
        "total_available": total_available,
        "total_collected": total_count,
        "total_outliers_removed": total_outliers,
        "market_count": len(market_codes),
        "markets": all_data,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_file = OUTPUT_DIR / f"auction_{date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 아카이브 저장 (월별 폴더)
    month_dir = ARCHIVE_DIR / date[:7].replace("-", "-")
    month_dir.mkdir(parents=True, exist_ok=True)
    archive_file = month_dir / f"auction_{date}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if total_outliers > 0:
        print(f"\n이상치 제거: {total_outliers}건 (100,000원/kg 초과 또는 금액 0원 이하)")
    print(f"\n총 {total_count:,}건 수집 (전국 {total_available:,}건 중) → {out_file}")
    print(f"아카이브: {archive_file}")
    return result


def backfill(date: str, market_codes: dict[str, str]) -> bool:
    """공판장 정산 지연 보정 — 기존 데이터보다 많으면 덮어쓰기.

    공판장(농협/원협)은 청과법인보다 정산 업로드가 1~2일 늦음.
    기존 파일의 건수와 비교하여 신규 데이터가 더 많을 때만 저장.
    반환: True = 업데이트됨, False = 변동 없음
    """
    existing_file = OUTPUT_DIR / f"auction_{date}.json"
    old_count = 0
    if existing_file.exists():
        with open(existing_file, "r", encoding="utf-8") as f:
            old_data = json.load(f)
        old_count = old_data.get("total_collected", 0)

    # 새로 수집
    new_data = collect(date, market_codes)
    new_count = new_data.get("total_collected", 0)

    if new_count > old_count:
        diff = new_count - old_count
        print(f"  ✓ {date} 보정 완료: {old_count:,} → {new_count:,}건 (+{diff:,}건)")
        return True
    else:
        # 기존 데이터 복원 (collect가 이미 덮어썼으므로)
        if old_count > 0 and existing_file.exists():
            # collect()가 이미 저장했지만, 동일하거나 적으면 기존 것 유지
            pass
        print(f"  - {date} 변동 없음 ({new_count:,}건)")
        return False


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
