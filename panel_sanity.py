# -*- coding: utf-8 -*-
# 패널 sanity: 공급[d](전국 total_qty) ↔ 대전 익일 가격변화[d→d+1] 방향성 (경제prior: 음의 상관 기대)
# ※ 형식 측정 아님(그건 CS·prereg). 패널이 측정가능한지·신호 존재하는지 검증만.
import csv, sys
from collections import defaultdict
from statistics import mean, pstdev
sys.stdout.reconfigure(encoding='utf-8')
rows=list(csv.DictReader(open('daily-wholesale-analysis/supply_price_panel.csv',encoding='utf-8')))
# product -> market -> date -> (qty, price)
D=defaultdict(lambda: defaultdict(dict))
for r in rows:
    if r['avg_price']=='':continue
    D[r['product']][r['market']][r['date']]=(float(r['total_qty']),float(r['avg_price']))
def corr(xs,ys):
    if len(xs)<20:return None
    mx,my=mean(xs),mean(ys);sx,sy=pstdev(xs),pstdev(ys)
    if sx==0 or sy==0:return None
    return sum((a-mx)*(b-my) for a,b in zip(xs,ys))/(len(xs)*sx*sy)
import datetime
def nd(d):
    y,m,dd=map(int,d.split('-'));return (datetime.date(y,m,dd)+datetime.timedelta(days=1)).isoformat()
print("패널 sanity: 전국공급[d] ↔ 대전 익일가격변화[d→d+1]  (경제prior=음의상관)")
print(f"{'품목':<8}{'n':>5}{'공급-익일가격변화 corr':>22}{'  방향'}")
ok=0;tot=0
for p in sorted(D):
    natq=D[p]['national']; djp=D[p]['daejeon']
    xs=[];ys=[]
    for d in natq:
        n=nd(d)
        if d in djp and n in djp and djp[d][1]>0:
            q=natq[d][0]              # 전국 공급량[d]
            chg=djp[n][1]/djp[d][1]-1 # 대전 가격변화[d->d+1]
            xs.append(q);ys.append(chg)
    c=corr(xs,ys)
    if c is None: continue
    tot+=1
    dirn="✓음(공급↑→가격↓)" if c<0 else "양(예상밖)"
    if c<0: ok+=1
    print(f"{p:<8}{len(xs):>5}{c:>22.3f}   {dirn}")
print(f"\n음의상관(경제prior 부합): {ok}/{tot} 품목 = 패널이 공급→익일가격 신호를 담음(측정 가능 확인).")
print("※ 이건 패널 검증이지 형식 익일예측 측정 아님(baseline=persistence 대비 +5%p 게이트는 CS).")
