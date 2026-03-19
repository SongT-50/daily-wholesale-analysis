"""공통 데이터 로더 — data/ 먼저, 없으면 아카이브에서 찾기"""
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"


def load_data(date: str) -> dict | None:
    """날짜별 경매 데이터 로드. data/ → 아카이브 순서로 탐색."""
    # 1. 로컬 data/ 폴더
    f = DATA_DIR / f"auction_{date}.json"
    if f.exists():
        with open(f, "r", encoding="utf-8") as fp:
            return json.load(fp)

    # 2. 아카이브 (월별 폴더)
    month = date[:7]
    f = ARCHIVE_DIR / month / f"auction_{date}.json"
    if f.exists():
        with open(f, "r", encoding="utf-8") as fp:
            return json.load(fp)

    return None


def load_shipment(date: str) -> dict | None:
    """날짜별 전자송품장 출하예약 데이터 로드. data/ → 아카이브 순서로 탐색."""
    # 1. 로컬 data/ 폴더
    f = DATA_DIR / f"shipment_{date}.json"
    if f.exists():
        with open(f, "r", encoding="utf-8") as fp:
            return json.load(fp)

    # 2. 아카이브 (월별 폴더)
    month = date[:7]
    f = ARCHIVE_DIR / month / f"shipment_{date}.json"
    if f.exists():
        with open(f, "r", encoding="utf-8") as fp:
            return json.load(fp)

    return None
