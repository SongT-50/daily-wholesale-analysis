# OA-13419 가락 경매전반입 -> 대전 익일 낙찰가 예측력 백테스트
# PREREG 개정1b 해시 1503b0415c5f139ab72c36b56b346210678509bc 설계 그대로.
# expanding-window 탈계절(train-only climatology) + walk-forward logistic + PC+/NC 대조군 + 5pp 게이트.
import csv, json, math, random
from collections import defaultdict
random.seed(20260724)  # NC 순열 재현

ARR="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/oa13419_garak_arrival.csv"
PANEL="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/supply_price_panel.csv"
OUTJSON="C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/RESULT_oa13419_backtest_2026-07-24.json"
PREREG_HASH="1503b0415c5f139ab72c36b56b346210678509bc"
P18=['감귤','감자','깻잎','당근','대파','딸기','마늘','무','배추','사과','상추','시금치','양배추','양파','오이','토마토','파프리카','포도']
GATE=5.0  # +5pp
WARMUP_DAYS=504  # 품목별 첫 ~2년 예측 제외
BLOCK=20         # walk-forward 테스트 블록 (expanding train)

def d8(s):  # 'YYYYMMDD' or 'YYYY-MM-DD' -> 'YYYY-MM-DD'
    s=s.strip().replace("-","")
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"

# --- 반입 TOT[품목,date] (18품목 exact GUBUN) ---
arr=defaultdict(dict)  # prod -> {date: tot}
with open(ARR,encoding="utf-8") as f:
    for r in csv.DictReader(f):
        g=r["GUBUN"].strip()
        if g not in P18: continue
        try: t=float(r["TOT"] or 0)
        except: continue
        arr[g][d8(r["TODATE"])]=t

# --- 대전 가격[품목,date] ---
price=defaultdict(dict)
with open(PANEL,encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["market"]!="daejeon": continue
        p=r["product"].strip()
        if p not in P18: continue
        try: v=float(r["avg_price"] or 0)
        except: continue
        if v>0: price[p][r["date"]]=v

# --- 로지스틱 (numpy 무의존, 표준화+GD) ---
def fit_logit(X,y,iters=300,lr=0.3):
    n=len(X); m=len(X[0]) if n else 0
    # 표준화
    mu=[sum(x[j] for x in X)/n for j in range(m)]
    sd=[(sum((x[j]-mu[j])**2 for x in X)/n)**0.5 or 1.0 for j in range(m)]
    Xs=[[(x[j]-mu[j])/sd[j] for j in range(m)] for x in X]
    w=[0.0]*m; b=0.0
    for _ in range(iters):
        gw=[0.0]*m; gb=0.0
        for i in range(n):
            z=b+sum(w[j]*Xs[i][j] for j in range(m))
            p=1/(1+math.exp(-max(-30,min(30,z))))
            e=p-y[i]
            for j in range(m): gw[j]+=e*Xs[i][j]
            gb+=e
        for j in range(m): w[j]-=lr*gw[j]/n
        b-=lr*gb/n
    return (w,b,mu,sd)
def pred(model,x):
    w,b,mu,sd=model
    z=b+sum(w[j]*((x[j]-mu[j])/sd[j]) for j in range(len(w)))
    return 1 if z>=0 else 0

def month_clim(vals_by_date, dates_train):
    # 월별 mean/std (train-only)
    by_m=defaultdict(list)
    for dt in dates_train:
        by_m[dt[5:7]].append(vals_by_date[dt])
    out={}
    for mo,vs in by_m.items():
        mu=sum(vs)/len(vs); sd=(sum((v-mu)**2 for v in vs)/len(vs))**0.5 or 1.0
        out[mo]=(mu,sd,len(vs))
    return out

def run(mode):
    # mode: 'base','test','pcplus','nc'
    tot_hit=0; tot_n=0; per={}
    for prod in P18:
        pa=arr.get(prod,{}); pp=price.get(prod,{})
        common=sorted(set(pa)&set(pp))
        if len(common)<WARMUP_DAYS+BLOCK+5:
            per[prod]={"n":0}; continue
        # NC: 반입 날짜 순열 (t->t+1 정렬 파괴)
        arr_series=dict(pa)
        if mode=="nc":
            vals=[pa[d] for d in common]; random.shuffle(vals)
            arr_series={d:v for d,v in zip(common,vals)}
        hit=0; n=0
        i=WARMUP_DAYS
        while i < len(common)-1:
            blk=common[i:i+BLOCK]
            train_dates=common[:i]
            # train-only climatology
            pc=month_clim(pp,train_dates); ac=month_clim({d:arr_series[d] for d in common},train_dates)
            def zp(dt):
                mo=dt[5:7];
                if mo not in pc: return None
                mu,sd,_=pc[mo]; return (pp[dt]-mu)/sd
            def za(dt):
                mo=dt[5:7]
                if mo not in ac: return None
                mu,sd,_=ac[mo]; return (arr_series[dt]-mu)/sd
            # 학습셋 구성
            X=[];Y=[]
            for k in range(1,len(train_dates)-1):
                d0,d1,d2=train_dates[k-1],train_dates[k],train_dates[k+1]
                if (d1[:7]!=d2[:7] and False): pass
                zp0,zp1,zp2=zp(d0),zp(d1),zp(d2); za1=za(d1); za0=za(d0)
                if None in (zp0,zp1,zp2,za1,za0): continue
                y=1 if (zp2-zp1)>0 else 0
                base=[zp1,zp1-zp0]
                if mode=="base": feat=base
                elif mode=="nc": feat=base+[za1,za1-za0]
                elif mode=="test": feat=base+[za1,za1-za0]
                elif mode=="pcplus": feat=base+[float(y)]  # 누설 오라클
                X.append(feat);Y.append(y)
            if len(X)<40 or len(set(Y))<2: i+=BLOCK; continue
            model=fit_logit(X,Y)
            # 예측 (test 블록)
            for k in range(len(blk)-1):
                d1=blk[k]; d2=blk[k+1]
                idx=common.index(d1)
                d0=common[idx-1]
                zp0,zp1,zp2=zp(d0),zp(d1),zp(d2); za1=za(d1); za0=za(d0)
                if None in (zp0,zp1,zp2,za1,za0): continue
                y=1 if (zp2-zp1)>0 else 0
                base=[zp1,zp1-zp0]
                if mode=="base": feat=base
                elif mode in ("nc","test"): feat=base+[za1,za1-za0]
                elif mode=="pcplus": feat=base+[float(y)]
                yh=pred(model,feat)
                hit+= (yh==y); n+=1
            i+=BLOCK
        per[prod]={"n":n,"hit":hit,"acc":round(100*hit/n,2) if n else None}
        tot_hit+=hit; tot_n+=n
    acc=round(100*tot_hit/tot_n,2) if tot_n else None
    return {"mode":mode,"pooled_acc":acc,"n":tot_n,"per_product":per}

print("running base..."); base=run("base")
print("running test..."); test=run("test")
print("running pcplus (누설=반드시통과)..."); pcp=run("pcplus")
print("running nc (날짜순열=반드시미달)..."); nc=run("nc")

delta = round(test["pooled_acc"]-base["pooled_acc"],2) if (test["pooled_acc"] and base["pooled_acc"]) else None
pc_delta = round(pcp["pooled_acc"]-base["pooled_acc"],2) if pcp["pooled_acc"] else None
nc_delta = round(nc["pooled_acc"]-base["pooled_acc"],2) if nc["pooled_acc"] else None
controls_ok = (pc_delta is not None and pc_delta>=GATE) and (nc_delta is not None and nc_delta<GATE)
gate_pass = (delta is not None and delta>=GATE)
verdict = ("VALID_RESULT " + ("PASS" if gate_pass else "FAIL")) if controls_ok else "INVALID_HARNESS(대조군 어긋남)"

res={"prereg_hash":PREREG_HASH,"gate_pp":GATE,
     "baseline_acc":base["pooled_acc"],"test_acc":test["pooled_acc"],"test_minus_base_pp":delta,
     "PCplus_acc":pcp["pooled_acc"],"PCplus_minus_base_pp":pc_delta,
     "NC_acc":nc["pooled_acc"],"NC_minus_base_pp":nc_delta,
     "controls_ok":controls_ok,"gate_pass":gate_pass,"verdict":verdict,
     "n_pred":test["n"],"detail":{"base":base,"test":test,"pcplus":pcp,"nc":nc}}
json.dump(res,open(OUTJSON,"w",encoding="utf-8"),ensure_ascii=False,indent=1)
print("\n===== RESULT =====")
print(f"baseline={base['pooled_acc']}% test={test['pooled_acc']}% delta={delta}pp (게이트 +{GATE}pp) n={test['n']}")
print(f"PC+ (누설,반드시통과)={pcp['pooled_acc']}% (Δ{pc_delta}pp) | NC(순열,반드시미달)={nc['pooled_acc']}% (Δ{nc_delta}pp)")
print(f"controls_ok={controls_ok} | gate_pass={gate_pass} | VERDICT={verdict}")
print(f"JSON -> {OUTJSON}")
