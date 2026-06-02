"""8년치 아카이브(wholesale-data) 전체 무결성 스캔.

목적: 5/11 농협대전류 '데이터 누락'을 전수 탐지.
  - (A) 단독 법인 누락: 어떤 (시장,법인)이 자기 활동기간(첫출현~마지막출현) 안의
        영업일인데 그날만 빠진 경우. 신규개장/폐업 구간은 활동기간 밖이라 자동 제외.
  - (B) 날짜 단위 미완성: 시장은 대부분 영업했는데(시장수 정상) 총건수가 그 요일
        중앙값의 60% 미만으로 급감 → 백필 덜 된 채 굳은 날 의심.
  - (C) 시장 전체 0건(영업일): 그날 다른 시장은 정상인데 특정 시장만 0.

요약은 stdout, 상세는 audit_coverage_result.json.
실행: python audit_coverage.py [--archive <dir>] [--recent-skip 5]
"""
import json, glob, sys, argparse, statistics
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict, Counter

sys.stdout.reconfigure(encoding="utf-8")

ap = argparse.ArgumentParser()
ap.add_argument("--archive", default="C:/Users/samsung/2026/02/wholesale-data")
ap.add_argument("--recent-skip", type=int, default=5,
                help="최근 N영업일 제외(백필 진행 중이라 누락 아님)")
ap.add_argument("--out", default="audit_coverage_result.json")
args = ap.parse_args()

files = sorted(glob.glob(f"{args.archive}/*/auction_*.json"))
present = defaultdict(set)        # (mk,corp) -> set(date_str)
market_bizdays = defaultdict(set) # mk -> set(date_str) with >0 items
market_name = {}
date_total = {}                   # date_str -> total items
date_nmk = {}                     # date_str -> # markets with data
all_business = []                 # date_str with any data
parse_err = []

for fp in files:
    ds = Path(fp).stem.replace("auction_", "")
    if len(ds) != 10:
        continue
    try:
        d = date.fromisoformat(ds)
    except ValueError:
        continue
    try:
        data = json.load(open(fp, encoding="utf-8"))
    except Exception as e:
        parse_err.append([ds, str(e)[:50]]); continue
    mks = data.get("markets", {})
    tot = 0; nmk = 0
    for mk, m in mks.items():
        items = m.get("items", []) or []
        market_name[mk] = m.get("market_name", "")
        corps = {it.get("corp_code") for it in items if it.get("corp_code")}
        if corps:
            market_bizdays[mk].add(ds); nmk += 1
        for c in corps:
            present[(mk, c)].add(ds)
        tot += len(items)
    date_total[ds] = tot
    date_nmk[ds] = nmk
    if tot > 0:
        all_business.append(ds)

all_business.sort()
# 최근 N영업일은 백필 진행 중 → 제외
skip_recent = set(all_business[-args.recent_skip:]) if args.recent_skip else set()
business_set = set(all_business) - skip_recent

def wd(ds): return date.fromisoformat(ds).weekday()

# ── (A) 단독 법인 누락 (활동기간 내) ──
mk_corps = defaultdict(set)
for (mk, c) in present:
    mk_corps[mk].add(c)
single = []
for (mk, c), days in present.items():
    if len(days) < 10:   # 활동 적은 법인은 baseline 약함 → 스킵
        continue
    first, last = min(days), max(days)
    # 활동기간 내 그 시장 영업일 중 이 법인이 빠진 날
    for ds in market_bizdays[mk]:
        if first <= ds <= last and ds in business_set and ds not in days:
            single.append([ds, mk, market_name.get(mk, ""), c, len(days)])
single.sort()

# ── (B) 날짜 단위 미완성 (요일 중앙값 60% 미만) ──
by_wd = defaultdict(list)
for ds in business_set:
    by_wd[wd(ds)].append(date_total[ds])
wd_med = {w: statistics.median(v) for w, v in by_wd.items() if v}
date_anom = []
for ds in sorted(business_set):
    med = wd_med.get(wd(ds), 0)
    # 시장 수는 정상권(그 시기 흔한 수)인데 총건수만 급감 → 미완성
    if med and date_total[ds] < med * 0.6:
        date_anom.append([ds, date_total[ds], round(med), date_nmk[ds]])

# ── (C) 시장 전체 0건 (영업일) ──
mkt_zero = []
for mk in mk_corps:
    biz = market_bizdays[mk]
    if len(biz) < 30:
        continue
    first, last = min(biz), max(biz)
    for ds in sorted(business_set):
        if first <= ds <= last and ds not in biz:
            mkt_zero.append([ds, mk, market_name.get(mk, "")])

# ── 요약 ──
def year(ds): return ds[:4]
print(f"스캔 파일: {len(files)} / 영업일: {len(all_business)} / 파싱오류: {len(parse_err)}")
print(f"최근 {args.recent_skip}영업일 제외(백필중): {sorted(skip_recent)}")
print(f"\n[A] 단독 법인 누락(활동기간 내): {len(single)}건")
print("  연도별:", dict(sorted(Counter(year(s[0]) for s in single).items())))
print("  시장별 top10:", Counter(f'{s[2]}({s[1]})' for s in single).most_common(10))
print(f"\n[B] 날짜 단위 미완성(요일중앙값 60%↓): {len(date_anom)}건")
print("  연도별:", dict(sorted(Counter(year(a[0]) for a in date_anom).items())))
print(f"\n[C] 시장 전체 0건(영업일): {len(mkt_zero)}건")
print("  시장별 top10:", Counter(f'{z[2]}({z[1]})' for z in mkt_zero).most_common(10))

json.dump({
    "scanned_files": len(files), "business_days": len(all_business),
    "parse_errors": parse_err, "skip_recent": sorted(skip_recent),
    "single_corp_missing": single,
    "date_incomplete": date_anom,
    "market_zero": mkt_zero,
}, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(f"\n상세 저장: {args.out}")
