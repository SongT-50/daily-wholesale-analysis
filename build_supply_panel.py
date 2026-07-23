# -*- coding: utf-8 -*-
# 익일예측 측정용 데이터 패널: (date, market_group, product) -> total_qty(공급), amount, avg_price
# market_group: garak(110001) / daejeon(250001+250003) / national(전 시장 합)
# 목적: CS (B)익일예측 게이트 측정의 base 데이터. supply[d]->surge[d+1] 임의 측정 가능하게 flexible 패널.
# 견고성: 월별 처리+즉시 append+진행로그 (killed돼도 부분 보존·재개 가능)
import json, glob, os, sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ARCH="C:/Users/samsung/2026/02/wholesale-data"
OUTDIR="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis"
OUT=f"{OUTDIR}/supply_price_panel.csv"
LOG="C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/00db0015-45f3-4626-a813-9e0695065c80/scratchpad/panel_build.log"
GARAK={"110001"}; DAEJEON={"250001","250003"}
# 물량 큰 공통 품목 위주(패널 크기 관리) — 필요시 확장
PRODUCTS={"배추","무","양파","대파","사과","토마토","오이","깻잎","시금치","상추",
          "청양고추","애호박","감자","당근","파프리카","딸기","포도","감귤","양배추","마늘"}

def log(m):
    with open(LOG,"a",encoding='utf-8') as f: f.write(m+"\n")

# 이미 처리한 월 스킵(재개)
done_months=set()
if os.path.exists(OUT):
    with open(OUT,encoding='utf-8') as f:
        next(f,None)
        for line in f:
            done_months.add(line.split(",")[0][:7])
else:
    with open(OUT,"w",encoding='utf-8') as f:
        f.write("date,market,product,total_qty,total_amount,avg_price\n")

months=[]
for y in (2018,2019,2020,2021,2022,2023,2024,2025,2026):
    for m in range(1,13):
        if y==2026 and m>6: break
        months.append(f"{y}-{m:02d}")

for mo in months:
    if mo in done_months:
        log(f"skip {mo} (이미 처리)"); continue
    agg=defaultdict(lambda:[0.0,0.0])  # (date,grp,prod)->[qty,amt]
    nf=0
    for f in sorted(glob.glob(f"{ARCH}/{mo}/auction_*.json")):
        try: data=json.load(open(f,encoding='utf-8'))
        except: continue
        nf+=1
        for mk,obj in data.get('markets',{}).items():
            if mk in GARAK: grps=["garak","national"]
            elif mk in DAEJEON: grps=["daejeon","national"]
            else: grps=["national"]
            for it in obj.get('items',[]):
                p=it.get('product')
                if p not in PRODUCTS: continue
                d=it.get('settle_date'); a=it.get('total_amount',0) or 0; q=it.get('total_qty',0) or 0
                for g in grps:
                    k=(d,g,p); agg[k][0]+=q; agg[k][1]+=a
    with open(OUT,"a",encoding='utf-8') as fo:
        for (d,g,p),(q,a) in sorted(agg.items()):
            fo.write(f"{d},{g},{p},{round(q,1)},{int(a)},{round(a/q,1) if q else ''}\n")
    log(f"done {mo}: {nf}파일 {len(agg)}행")

# DEDUP FINAL: 재개/중단 중 월 중복append 방지(모든 값 동일이라 안전). 완료 후 1회 정리.
import csv as _csv
_seen=set();_out=[]
with open(OUT,encoding="utf-8") as _f:
    _rd=_csv.reader(_f);_h=next(_rd)
    for _row in _rd:
        _k=(_row[0],_row[1],_row[2])
        if _k in _seen:continue
        _seen.add(_k);_out.append(_row)
_out.sort(key=lambda r:(r[0],r[1],r[2]))
with open(OUT,"w",encoding="utf-8",newline="") as _f:
    _w=_csv.writer(_f);_w.writerow(_h);_w.writerows(_out)
log(f"DEDUP FINAL: {len(_out)}행")

log("ALL DONE")
