# OA-13419 가락 경매전반입 -> 대전 익일 낙찰가 예측력 백테스트 (numpy 벡터화)
# PREREG 개정1b 해시 1503b0415c5f139ab72c36b56b346210678509bc 설계 그대로. 구현만 벡터화(설계 불변).
# expanding train-only 월climatology z-score + walk-forward logistic + PC+/NC 대조군 + 5pp 게이트.
import csv, json
import numpy as np
from collections import defaultdict
rng=np.random.default_rng(20260724)

BASE="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/"
ARR=BASE+"oa13419_garak_arrival.csv"; PANEL=BASE+"supply_price_panel.csv"
OUTJSON=BASE+"RESULT_oa13419_backtest_2026-07-24.json"
PREREG_HASH="1503b0415c5f139ab72c36b56b346210678509bc"
P18=['감귤','감자','깻잎','당근','대파','딸기','마늘','무','배추','사과','상추','시금치','양배추','양파','오이','토마토','파프리카','포도']
GATE=5.0; WARMUP=504; BLOCK=20

def d8(s):
    s=s.strip().replace("-","");return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
arr=defaultdict(dict); price=defaultdict(dict)
with open(ARR,encoding="utf-8") as f:
    for r in csv.DictReader(f):
        g=r["GUBUN"].strip()
        if g in P18:
            try: arr[g][d8(r["TODATE"])]=float(r["TOT"] or 0)
            except: pass
with open(PANEL,encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["market"]=="daejeon" and r["product"].strip() in P18:
            try:
                v=float(r["avg_price"] or 0)
                if v>0: price[r["product"].strip()][r["date"]]=v
            except: pass

def logit_fit(X,y,iters=150,lr=0.5):
    mu=X.mean(0); sd=X.std(0); sd[sd==0]=1
    Xs=(X-mu)/sd; n,m=Xs.shape
    w=np.zeros(m); b=0.0
    for _ in range(iters):
        z=np.clip(Xs@w+b,-30,30); p=1/(1+np.exp(-z)); e=p-y
        w-=lr*(Xs.T@e)/n; b-=lr*e.mean()
    return w,b,mu,sd
def logit_pred(model,X):
    w,b,mu,sd=model
    return ((( (X-mu)/sd )@w+b)>=0).astype(int)

def zseries(vals,months,train_mask):
    # 월별 mean/std = train_mask=True 인 것만 (expanding train-only)
    z=np.full(len(vals),np.nan)
    for mo in range(1,13):
        tr=(months==mo)&train_mask
        if tr.sum()<3: continue
        mu=vals[tr].mean(); sd=vals[tr].std() or 1.0
        sel=(months==mo)
        z[sel]=(vals[sel]-mu)/sd
    return z

def run(mode):
    tot_hit=0; tot_n=0; per={}
    for prod in P18:
        pa=arr.get(prod,{}); pp=price.get(prod,{})
        common=sorted(set(pa)&set(pp))
        if len(common)<WARMUP+BLOCK+5: per[prod]={"n":0}; continue
        pv=np.array([pp[d] for d in common]); av=np.array([pa[d] for d in common])
        mon=np.array([int(d[5:7]) for d in common])
        if mode=="nc":
            perm=rng.permutation(len(av)); av=av[perm]
        N=len(common); hit=0; n=0; i=WARMUP
        while i<N-1:
            tr=np.zeros(N,bool); tr[:i]=True
            zp=zseries(pv,mon,tr); za=zseries(av,mon,tr)
            # 유효 인덱스(k-1,k,k+1 존재 + z 유효)
            def feat_at(k):
                if k<1 or k+1>=N: return None
                a=[zp[k-1],zp[k],zp[k+1],za[k],za[k-1]]
                if any(np.isnan(a)): return None
                return a
            # train: [1, i-1)
            Xtr=[];Ytr=[]
            for k in range(1,i-1):
                fa=feat_at(k)
                if fa is None: continue
                zp0,zp1,zp2,za1,za0=fa
                y=1 if (zp2-zp1)>0 else 0
                base=[zp1,zp1-zp0]
                if mode=="base": ft=base
                elif mode in ("test","nc"): ft=base+[za1,za1-za0]
                elif mode=="pcplus": ft=base+[float(y)]
                Xtr.append(ft);Ytr.append(y)
            if len(Xtr)<40 or len(set(Ytr))<2: i+=BLOCK; continue
            model=logit_fit(np.array(Xtr,float),np.array(Ytr,float))
            for k in range(i,min(i+BLOCK,N-1)):
                fa=feat_at(k)
                if fa is None: continue
                zp0,zp1,zp2,za1,za0=fa
                y=1 if (zp2-zp1)>0 else 0
                base=[zp1,zp1-zp0]
                if mode=="base": ft=base
                elif mode in ("test","nc"): ft=base+[za1,za1-za0]
                elif mode=="pcplus": ft=base+[float(y)]
                yh=logit_pred(model,np.array([ft],float))[0]
                hit+=int(yh==y); n+=1
            i+=BLOCK
        per[prod]={"n":n,"acc":round(100*hit/n,2) if n else None}
        tot_hit+=hit; tot_n+=n
    return {"mode":mode,"pooled_acc":round(100*tot_hit/tot_n,2) if tot_n else None,"n":tot_n,"per_product":per}

import sys
for lbl in ["base","test","pcplus","nc"]:
    print("run",lbl,flush=True)
res={m:run(m) for m in ["base","test","pcplus","nc"]}
base,test,pcp,nc=res["base"],res["test"],res["pcplus"],res["nc"]
def dd(a,b): return round(a-b,2) if (a is not None and b is not None) else None
delta=dd(test["pooled_acc"],base["pooled_acc"])
pc_d=dd(pcp["pooled_acc"],base["pooled_acc"]); nc_d=dd(nc["pooled_acc"],base["pooled_acc"])
controls_ok=(pc_d is not None and pc_d>=GATE) and (nc_d is not None and nc_d<GATE)
gate_pass=(delta is not None and delta>=GATE)
verdict=("VALID "+("PASS" if gate_pass else "FAIL")) if controls_ok else "INVALID_HARNESS(대조군어긋남)"
out={"prereg_hash":PREREG_HASH,"gate_pp":GATE,"baseline_acc":base["pooled_acc"],"test_acc":test["pooled_acc"],
     "test_minus_base_pp":delta,"PCplus_acc":pcp["pooled_acc"],"PCplus_minus_base_pp":pc_d,
     "NC_acc":nc["pooled_acc"],"NC_minus_base_pp":nc_d,"controls_ok":controls_ok,"gate_pass":gate_pass,
     "verdict":verdict,"n_pred":test["n"],"detail":res}
json.dump(out,open(OUTJSON,"w",encoding="utf-8"),ensure_ascii=False,indent=1)
print("\n===== RESULT =====",flush=True)
print(f"baseline={base['pooled_acc']}% test={test['pooled_acc']}% delta={delta}pp (gate +{GATE}) n={test['n']}",flush=True)
print(f"PC+={pcp['pooled_acc']}%(Δ{pc_d}) NC={nc['pooled_acc']}%(Δ{nc_d})",flush=True)
print(f"controls_ok={controls_ok} gate_pass={gate_pass} VERDICT={verdict}",flush=True)
