# -*- coding: utf-8 -*-
# 깨끗한 선행신호: 가락 공급[d] -> 대전 익일 가격변화[d+1] (독립시장, confound 없음)
# vs persistence(대전 전일 가격변화[d-1->d] -> [d->d+1]) 방향 정확도 비교 = CS 측정의 예고편(탐색)
import csv, sys
from collections import defaultdict
from statistics import mean, pstdev
sys.stdout.reconfigure(encoding='utf-8')
rows=list(csv.DictReader(open('daily-wholesale-analysis/supply_price_panel.csv',encoding='utf-8')))
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
print("깨끗한 선행신호: 가락공급[d] -> 대전 익일가격변화 (confound 없음)")
print(f"{'품목':<8}{'n':>5}{'가락공급→대전익일가격':>20}")
res=[]
for p in sorted(D):
    gq=D[p]['garak']; djp=D[p]['daejeon']
    xs=[];ys=[]
    for d in gq:
        n=nd(d)
        if d in djp and n in djp and djp[d][1]>0:
            xs.append(gq[d][0]); ys.append(djp[n][1]/djp[d][1]-1)
    c=corr(xs,ys)
    if c is None:continue
    res.append((p,len(xs),c))
    print(f"{p:<8}{len(xs):>5}{c:>20.3f}{'  ✓음' if c<0 else '  양'}")
neg=sum(1 for _,_,c in res if c<0)
print(f"\n음의상관: {neg}/{len(res)} (가락공급↑→대전익일가격↓, 경제prior). 이게 confound없는 선행신호 = CS가 써야 할 축.")
