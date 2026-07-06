import openpyxl, json, glob, sys
sys.path.insert(0,'C:/Users/samsung/2026/02/monet/daily-wholesale-analysis')
import settlement_report as sr
from collections import Counter, defaultdict
# 1) product -> category_code 사전 (노은 중앙, 2025+2026 전체)
tmp=defaultdict(Counter)
for y in (2025,2026):
    for m in range(1,7):
        for p in glob.glob(f'C:/Users/samsung/2026/02/wholesale-data/{y}-{m:02d}/auction_*.json'):
            if p.endswith('.bak') or __import__('os').path.getsize(p)<300_000: continue
            try: d=json.load(open(p,encoding='utf-8'))
            except: continue
            for it in d.get('markets',{}).get('250003',{}).get('items',[]) or []:
                if it.get('corp_code')=='25000301':
                    pr=it.get('product'); cc=it.get('category_code')
                    if pr: tmp[pr][cc]+=1
prod2cat={k:v.most_common(1)[0][0] for k,v in tmp.items()}
products=sorted(prod2cat, key=len, reverse=True)
# 수동 매핑 (짧은/애매 품목 — 1/6 대조 기반 상식)
MANUAL={'감':'단감','파':'대파','피망':'피망(단고추)','나물':'취나물','로즈':'로즈마리'}
def mapp(name):
    name=name.strip()
    if name in prod2cat: return name
    if name in MANUAL and MANUAL[name] in prod2cat: return MANUAL[name]
    # aT product가 회사품목으로 시작(회사명이 잘린 경우)
    cands=[p for p in products if p.startswith(name)]
    if cands: return cands[0]
    # 회사품목이 aT product로 시작
    cands=[p for p in products if name.startswith(p)]
    if cands: return cands[0]
    return None
wb=openpyxl.load_workbook('C:/Users/samsung/Downloads/데이터 누락 일자 자료.xlsx', data_only=True)
def agg(sheets):
    auc=defaultdict(lambda:[0.0,0.0]); tot=[0.0,0.0]; unmapped=defaultdict(float)
    for sh in sheets:
        for row in wb[sh].iter_rows(min_row=2, values_only=True):
            p=row[0]
            if p is None: continue
            p=str(p).strip()
            if p in ('-소계-','=합계='): continue
            q=float(row[2] or 0); a=float(row[3] or 0)
            tot[0]+=a; tot[1]+=q
            mp=mapp(p)
            if mp:
                idx=sr.auction_block_index(mp, prod2cat[mp])
                lab=sr.AUCTION_BLOCKS[idx][3] if idx<len(sr.AUCTION_BLOCKS) else '소실 미배분'
            else: lab='소실 미배분'; unmapped[p]+=a
            auc[lab][0]+=a; auc[lab][1]+=q
    return auc, tot, unmapped
for title,sheets in [('작년 소실(1/3·1/4·1/7·1/9)',['25.1.3','25.1.4','25.1.7','25.1.9']),('올해 소실(2/24)',['26.2.24'])]:
    auc,tot,unm=agg(sheets)
    print(f"=== {title}: 총 {tot[0]/1e8:.2f}억 / {tot[1]/1000:.0f}톤 ===")
    for lab in sorted(auc, key=lambda L: sr._LABEL_ORDER.get(L,999)):
        a,q=auc[lab]
        print(f"  {sr.clean_label(lab) if hasattr(sr,'clean_label') else lab:26s} {a/1e8:6.2f}억 {q/1000:6.0f}톤")
    if unm:
        print(f"  [미배분 품목 {len(unm)}종] 금액 {sum(unm.values())/1e8:.2f}억:", list(unm.keys())[:12])
    print()

# === aT 실측 + 소실 배분 = 경매사별 실제값 → JSON ===
import os
aT=json.load(open(f'{os.path.dirname(os.path.abspath(__file__))}/auctioneer_result.json',encoding='utf-8'))
prev_auc,_,prev_unm=agg(['25.1.3','25.1.4','25.1.7','25.1.9'])
cur_auc,_,cur_unm=agg(['26.2.24'])
def gj(auc,lab): 
    v=auc.get(lab,[0,0]); return v[0]/1e8, v[1]/1000
out=[]
for r in aT['rows']:
    lab=r['auctioneer']
    pa_add,pq_add=gj(prev_auc,lab); ca_add,cq_add=gj(cur_auc,lab)
    out.append({'n':lab,
      'ca':round(r['cur_amt']+ca_add,2),'pa':round(r['prev_amt']+pa_add,2),
      'cq':round(r['cur_qty']+cq_add),'pq':round(r['prev_qty']+pq_add)})
# 소실 미배분 행
pa_u,pq_u=gj(prev_auc,'소실 미배분'); ca_u,cq_u=gj(cur_auc,'소실 미배분')
out.append({'n':'소실 미배분','ca':round(ca_u,2),'pa':round(pa_u,2),'cq':round(cq_u),'pq':round(pq_u)})
tca=sum(o['ca'] for o in out); tpa=sum(o['pa'] for o in out)
tcq=sum(o['cq'] for o in out); tpq=sum(o['pq'] for o in out)
print("\n=== 경매사별 실제값 (aT + 소실배분) ===")
for o in out:
    d=o['ca']-o['pa']
    print(f"  {sr.clean_label(o['n']) if hasattr(sr,'clean_label') else o['n']:26s} 올해{o['ca']:7.1f} 작년{o['pa']:7.1f} Δ{d:+6.1f} | 물량 올해{o['cq']:>6} 작년{o['pq']:>6}")
print(f"\n합계: 올해 {tca:.1f}억/{tcq:,}톤 | 작년 {tpa:.1f}억/{tpq:,}톤 (월계표 835.7/881.4·38241/37101 대조)")
SP_OUT='C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/7136e200-519f-41e9-9c67-5d75ce5467f5/scratchpad/auctioneer_real.json'
json.dump(out,open(SP_OUT,'w',encoding='utf-8'),ensure_ascii=False,indent=1)
print("저장:",SP_OUT)
