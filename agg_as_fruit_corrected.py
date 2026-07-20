import sys, json
sys.path.insert(0,"C:/Users/samsung/2026/02/monet/daily-wholesale-analysis")
from datetime import date
from collections import defaultdict
import pandas as pd
from settlement_report import load_range
J,W="25000301","25000302"
AS15={"듀리안","레몬","망고","망고스턴","바나나","아로니아","아보카도","오렌지","용과","자몽","참다래(키위)","체리","코코넛","탄제린","파인애플"}
ASX=AS15|{"다래","수입땅콩","수입곶감","수입호두"}
XLS={4:"C:/Users/samsung/Downloads/도매시장 거래현황 2026-04-01-2026-04-30.xls",
     5:"C:/Users/samsung/Downloads/도매시장 거래현황 2026-05-01-2026-05-31.xls"}
def num(v):
    try:return float(v)
    except:return 0.0
recs,days=load_range(date(2026,1,1),date(2026,6,30))
central=defaultdict(lambda:[0.0,0.0]);won=defaultdict(lambda:[0.0,0.0])
for r in recs:
    cc=r.get("corp_code","");p=r.get("product","")
    if p not in ASX: continue
    a=num(r.get("total_amount"));q=num(r.get("total_qty"))
    sd=str(r.get("settle_date") or "");m=sd[5:7]
    if cc==J: central[p][0]+=a;central[p][1]+=q
    elif cc==W and m in ("01","02","03","06"):  # 원협 1·2·3·6월만 아카이브 (4·5월은 .xls로 교체)
        won[p][0]+=a;won[p][1]+=q
# 원협 4·5월 = .xls 실거래
for mm in (4,5):
    df=pd.read_excel(XLS[mm])
    for _,row in df.iterrows():
        prod=str(row['품목']).strip();var=str(row.get('품종','')).strip()
        a=num(row['금액(원)']);q=num(row['물량(kg)'])
        key=None
        if prod in AS15 or prod=="다래": key=prod
        elif prod=="땅콩" and "수입" in var: key="수입땅콩"
        elif prod=="곶감" and "수입" in var: key="수입곶감"
        elif prod=="호두" and "수입" in var: key="수입호두"
        if key: won[key][0]+=a;won[key][1]+=q
prods=sorted(set(central)|set(won), key=lambda p:-(central[p][1]+won[p][1]))
rows=[{"product":p,"c":central[p],"w":won[p]} for p in prods]
json.dump({"rows":rows,"days":days},open("C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/42e5fa00-ba61-4d87-863d-64f49b83cc1e/scratchpad/as_corrected.json","w",encoding="utf-8"),ensure_ascii=False)
cs=sum(r["c"][1] for r in rows);ws=sum(r["w"][1] for r in rows)
ca=sum(r["c"][0] for r in rows);wa=sum(r["w"][0] for r in rows)
print(f"DONE 중앙 {ca/1e8:.1f}억/{cs/1000:.0f}톤 · 원협 {wa/1e8:.1f}억/{ws/1000:.0f}톤 · 원협금액비중 {wa/(ca+wa)*100:.1f}%")
