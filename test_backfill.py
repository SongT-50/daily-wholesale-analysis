"""backfill() 단위테스트 — 윈도우 확대(D+5)에 따른 안정화 skip + 손실 방지 복원.

검증:
  1) 안정화 skip: 대상일+5일 이후 수집된 데이터는 collect() 호출 없이 skip
  2) 갱신: 새 수집이 더 많으면 교체(True)
  3) 손실 방지: 새 수집이 더 적으면 data/ + 아카이브 둘 다 원본 복원(False)
     (구버전 `pass` 버그 = 적은 데이터로 덮인 채 방치되던 것 수정 확인)

실행: python test_backfill.py
"""
import json, sys, tempfile
from pathlib import Path
import collect as C

sys.stdout.reconfigure(encoding="utf-8")

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ✅ {name}")
    else: FAIL += 1; print(f"  ❌ {name}  {detail}")

def _doc(date_str, total, collected_at):
    return {"date": date_str, "total_collected": total, "collected_at": collected_at,
            "markets": {"250001": {"market_name": "대전오정", "items": [{"corp_code": "x"}] * total}}}

def write_existing(date_str, total, collected_at):
    (C.OUTPUT_DIR / f"auction_{date_str}.json").write_text(
        json.dumps(_doc(date_str, total, collected_at)), encoding="utf-8")
    ad = C.ARCHIVE_DIR / date_str[:7]; ad.mkdir(parents=True, exist_ok=True)
    (ad / f"auction_{date_str}.json").write_text(
        json.dumps(_doc(date_str, total, collected_at)), encoding="utf-8")

CALLS = []
def make_fake_collect(new_total):
    def fc(date_str, market_codes):
        CALLS.append(date_str)
        doc = _doc(date_str, new_total, "2026-06-02T10:00:00")
        (C.OUTPUT_DIR / f"auction_{date_str}.json").write_text(json.dumps(doc), encoding="utf-8")
        ad = C.ARCHIVE_DIR / date_str[:7]; ad.mkdir(parents=True, exist_ok=True)
        (ad / f"auction_{date_str}.json").write_text(json.dumps(doc), encoding="utf-8")
        return doc
    return fc

def total_in(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))["total_collected"]

tmp = Path(tempfile.mkdtemp(prefix="bf_test_"))
C.OUTPUT_DIR = tmp / "data"; C.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
C.ARCHIVE_DIR = tmp / "arch"

print("[1] 안정화 skip (대상일+5일 이후 수집 → 재수집 안 함)")
CALLS.clear()
write_existing("2026-05-11", 1800, "2026-05-16T09:00:00")  # D+5
C.collect = make_fake_collect(9999)
r = C.backfill("2026-05-11", {"250001": "대전오정"})
check("collect 미호출(skip)", CALLS == [], f"calls={CALLS}")
check("반환 False", r is False)
check("기존값 유지(1800)", total_in(C.OUTPUT_DIR / "auction_2026-05-11.json") == 1800)

print("[2] 갱신 (새 수집이 더 많음)")
CALLS.clear()
write_existing("2026-05-12", 500, "2026-05-13T23:00:00")  # D+1, 미안정
C.collect = make_fake_collect(1828)
r = C.backfill("2026-05-12", {"250001": "대전오정"})
check("collect 호출", CALLS == ["2026-05-12"])
check("반환 True", r is True)
check("새값 반영(1828)", total_in(C.OUTPUT_DIR / "auction_2026-05-12.json") == 1828)
check("아카이브도 1828", total_in(C.ARCHIVE_DIR / "2026-05" / "auction_2026-05-12.json") == 1828)

print("[3] 손실 방지 복원 (새 수집이 더 적음 → 원본 유지)")
CALLS.clear()
write_existing("2026-05-13", 1800, "2026-05-15T23:00:00")  # D+2, 미안정
C.collect = make_fake_collect(500)  # API 일시오류로 적게 옴
r = C.backfill("2026-05-13", {"250001": "대전오정"})
check("collect 호출됨", CALLS == ["2026-05-13"])
check("반환 False", r is False)
check("data/ 원본 복원(1800)", total_in(C.OUTPUT_DIR / "auction_2026-05-13.json") == 1800)
check("아카이브 원본 복원(1800)", total_in(C.ARCHIVE_DIR / "2026-05" / "auction_2026-05-13.json") == 1800)

print("[4] 신규일 (기존 없음 → 무조건 수집)")
CALLS.clear()
C.collect = make_fake_collect(30000)
r = C.backfill("2026-05-20", {"250001": "대전오정"})
check("collect 호출", CALLS == ["2026-05-20"])
check("반환 True", r is True)

print(f"\n{'='*46}\n결과: {PASS} PASS / {FAIL} FAIL")
sys.exit(1 if FAIL else 0)
