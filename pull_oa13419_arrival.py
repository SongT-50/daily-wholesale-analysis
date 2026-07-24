# OA-13419 가락 경매전 반입(GarakAuctionBefore) 전량 pull -> CSV
# 반입 데이터만 저장(가격 조인 X = PREREG 무결성 유지). CS 계측기 게이트 + 백테스트 base용.
import os, json, urllib.request, csv, time
from dotenv import load_dotenv
load_dotenv("C:/Users/samsung/2026/02/monet/.env")
KEY=os.environ["SEOUL_OPENAPI_KEY"]; SERVICE="GarakAuctionBefore"
OUT="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/oa13419_garak_arrival.csv"
LOG="C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/cb80ca4b-a91f-403c-bb07-9751ba80601f/scratchpad/oa13419_pull.log"
def log(m):
    with open(LOG,"a",encoding="utf-8") as f: f.write(m+"\n")
    print(m)
def pull(a,b):
    url=f"http://openAPI.seoul.go.kr:8088/{KEY}/json/{SERVICE}/{a}/{b}/"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)[SERVICE]
        except Exception as e:
            log(f"  retry {attempt} {a}-{b}: {type(e).__name__}")
            time.sleep(2)
    raise RuntimeError(f"fail {a}-{b}")
first=pull(1,1)
total=int(first["list_total_count"])
cols=["TODATE","BURYU","GUBUN","A1","A2","A3","A4","A5","A6","A7","TOT"]
n=0
with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f); w.writerow(cols)
    s=1
    while s<=total:
        e=min(s+999,total)
        blk=pull(s,e)["row"]
        for r in blk:
            w.writerow([r.get(c,"") for c in cols])
        n+=len(blk)
        if s%20000==1: log(f"  {n}/{total}")
        s=e+1
log(f"DONE {n}행 -> oa13419_garak_arrival.csv")
