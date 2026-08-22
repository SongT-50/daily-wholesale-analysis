from pathlib import Path as _Path  # 경로는 파일 위치에서 얻는다(하드코딩 금지)
_MONET = _Path(__file__).resolve().parent.parent.as_posix()
from pathlib import Path as _Path  # 경로는 파일 위치·환경에서 얻는다(하드코딩 금지)
import tempfile as _tempfile
_TMP = _Path(_tempfile.gettempdir()).as_posix()
# OA-13419 가락 경매전 반입(GarakAuctionBefore) 전량 pull -> CSV
# 반입 데이터만 저장(가격 조인 X = PREREG 무결성 유지). CS 계측기 게이트 + 백테스트 base용.
import os, json, urllib.request, csv, time
from dotenv import load_dotenv
load_dotenv(f"{_MONET}/.env")
KEY=os.environ["SEOUL_OPENAPI_KEY"]; SERVICE="GarakAuctionBefore"
OUT=f"{_MONET}/daily-wholesale-analysis/oa13419_garak_arrival.csv"
LOG=f"{_TMP}/oa13419_pull.log"
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
