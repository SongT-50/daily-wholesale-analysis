# OA-13419 가락 경매전반입 -> 대전 익일 가격 surge/drop 예측력 백테스트 v2 (지표 재정렬)
# PREREG 개정2 = PREREG_oa13419_v2_metric_realign_2026-07-24.md (결과 前 동결).
# v1 대비: 라벨=raw 익일 surge/drop 이벤트 / 지표=balanced accuracy / surge더미 3요소 / class-weighted / 부호검정+부트스트랩.
# 분류기·expanding train-only climatology·WARMUP·BLOCK = v1 유지(CS 사인오프분).
# D+4꼬리제외(TAIL_EXCL) = PREREG §3 등록분이나 v1·초판 v2 코드 누락 → 2026-07-24 G-E catch로 구현 보정(앵커 정합). 영향 측정=미미(surge Δ1.53→1.42·drop 1.17→1.12, 판정 불변).
import csv, json
import numpy as np
from collections import defaultdict
rng = np.random.default_rng(20260724)

BASE = "C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/"
ARR = BASE + "oa13419_garak_arrival.csv"; PANEL = BASE + "supply_price_panel.csv"
OUTJSON = BASE + "RESULT_oa13419_v2_2026-07-24.json"
PREREG = "PREREG_oa13419_v2_metric_realign_2026-07-24.md"
P18 = ['감귤','감자','깻잎','당근','대파','딸기','마늘','무','배추','사과','상추','시금치','양배추','양파','오이','토마토','파프리카','포도']
PT = 5.0          # 가격 이벤트 임계 (H0 v2 v4.PRICE_THRESHOLD, 새로 안 정함)
GATE = 5.0        # delta_bacc 게이트 pp
SURGE_Q = 90.0    # surge더미: za train 90분위(상위10%) — CS 위임분, 결과前 동결
MAX_GAP = 4; WARMUP = 504; BLOCK = 20
TAIL_EXCL = 5     # D+4 꼬리(정산 미완비) 제외 = 마지막 5 거래일 (앵커 정합, PREREG §3 등록분. 2026-07-24 G-E catch로 구현 보정)
BOOT = 2000

def d8(s):
    s = s.strip().replace("-", ""); return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
def dgap(a, b):
    from datetime import date
    da = date(int(a[:4]), int(a[5:7]), int(a[8:10])); db = date(int(b[:4]), int(b[5:7]), int(b[8:10]))
    return abs((da - db).days)

arr = defaultdict(dict); price = defaultdict(dict)
with open(ARR, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        g = r["GUBUN"].strip()
        if g in P18:
            try: arr[g][d8(r["TODATE"])] = float(r["TOT"] or 0)
            except: pass
with open(PANEL, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["market"] == "daejeon" and r["product"].strip() in P18:
            try:
                v = float(r["avg_price"] or 0)
                if v > 0: price[r["product"].strip()][r["date"]] = v
            except: pass

def logit_fit(X, y, sw, iters=150, lr=0.5):
    # class-weighted (sw=표본가중). 표준화는 비가중.
    mu = X.mean(0); sd = X.std(0); sd[sd == 0] = 1
    Xs = (X - mu) / sd; n, m = Xs.shape
    w = np.zeros(m); b = 0.0; W = sw.sum()
    for _ in range(iters):
        z = np.clip(Xs @ w + b, -30, 30); p = 1 / (1 + np.exp(-z)); e = (p - y) * sw
        w -= lr * (Xs.T @ e) / W; b -= lr * e.sum() / W
    return w, b, mu, sd
def logit_pred(model, X):
    w, b, mu, sd = model
    return ((((X - mu) / sd) @ w + b) >= 0).astype(int)

def zseries(vals, months, train_mask):
    z = np.full(len(vals), np.nan)
    for mo in range(1, 13):
        tr = (months == mo) & train_mask
        if tr.sum() < 3: continue
        mu = vals[tr].mean(); sd = vals[tr].std() or 1.0
        z[(months == mo)] = (vals[(months == mo)] - mu) / sd
    return z

def event(pctv, mode):
    return 1 if (pctv > PT if mode == "surge" else pctv < -PT) else 0

def measure_product(prod, mode, arm):
    """arm in {base,test,pcplus,nc}. return per-product balanced accuracy (or None)."""
    pa = arr.get(prod, {}); pp = price.get(prod, {})
    common = sorted(set(pa) & set(pp))
    if len(common) < WARMUP + BLOCK + 5: return None
    N = len(common)
    pv = np.array([pp[d] for d in common]); av = np.array([pa[d] for d in common])
    mon = np.array([int(d[5:7]) for d in common])
    # raw 대전 익일 변화율 pct[k] (전 common일 대비, 실제 날짜갭≤MAX_GAP)
    pct = np.full(N, np.nan)
    for k in range(1, N):
        if dgap(common[k], common[k-1]) <= MAX_GAP and pv[k-1] > 0:
            pct[k] = (pv[k] - pv[k-1]) / pv[k-1] * 100.0
    if arm == "nc":
        av = av[rng.permutation(N)]
    n_use = N - TAIL_EXCL  # 꼬리 제외: 마지막 5거래일은 예측지점에서 제외(정산 미완비 오염 방지)
    TP = FN = TN = FP = 0
    i = WARMUP
    while i < n_use - 1:
        tr = np.zeros(N, bool); tr[:i] = True
        zp = zseries(pv, mon, tr); za = zseries(av, mon, tr)
        # surge더미 임계 = train za의 SURGE_Q 분위 (expanding train-only)
        za_tr = za[tr]; za_tr = za_tr[~np.isnan(za_tr)]
        qz = np.percentile(za_tr, SURGE_Q) if len(za_tr) >= 10 else np.inf

        def feat(k):
            # 예측지점 k → 라벨=익일 pct[k+1] 이벤트. AR/supply 특징은 t=k까지.
            if k < 1 or k + 1 >= N: return None
            if np.isnan(pct[k+1]): return None
            a = [zp[k-1], zp[k], za[k-1], za[k]]
            if any(np.isnan(a)): return None
            y = event(pct[k+1], mode)
            base = [zp[k], zp[k] - zp[k-1]]
            if arm == "base":
                ft = base
            elif arm in ("test", "nc"):
                sd_dummy = 1.0 if za[k] > qz else 0.0
                ft = base + [za[k], za[k] - za[k-1], sd_dummy]
            elif arm == "pcplus":
                ft = base + [float(y)]
            return ft, y

        Xtr = []; Ytr = []
        for k in range(1, i - 1):
            r = feat(k)
            if r is None: continue
            Xtr.append(r[0]); Ytr.append(r[1])
        if len(Xtr) < 40 or len(set(Ytr)) < 2:
            i += BLOCK; continue
        Xtr = np.array(Xtr, float); Ytr = np.array(Ytr, float)
        npos = Ytr.sum(); nneg = len(Ytr) - npos
        # class weight = 역빈도 (양쪽 클래스 총가중 동일)
        sw = np.where(Ytr == 1, len(Ytr) / (2 * npos), len(Ytr) / (2 * nneg))
        model = logit_fit(Xtr, Ytr, sw)
        for k in range(i, min(i + BLOCK, n_use - 1)):
            r = feat(k)
            if r is None: continue
            ft, y = r
            yh = logit_pred(model, np.array([ft], float))[0]
            if y == 1:
                TP += int(yh == 1); FN += int(yh == 0)
            else:
                TN += int(yh == 0); FP += int(yh == 1)
        i += BLOCK
    if (TP + FN) == 0 or (TN + FP) == 0:
        return None  # 이벤트/비이벤트 한쪽 없음 = bacc 정의불가
    tpr = TP / (TP + FN); tnr = TN / (TN + FP)
    return round(100 * 0.5 * (tpr + tnr), 2)

def run_mode(mode):
    out = {"base": {}, "test": {}, "pcplus": {}, "nc": {}}
    for prod in P18:
        for arm in ("base", "test", "pcplus", "nc"):
            b = measure_product(prod, mode, arm)
            if b is not None:
                out[arm][prod] = b
    # 공통 품목(base·test 둘 다 유효)만 delta
    common_p = sorted(set(out["base"]) & set(out["test"]))
    deltas = {p: round(out["test"][p] - out["base"][p], 2) for p in common_p}
    mean_ar = np.mean([out["base"][p] for p in common_p]) if common_p else None
    mean_test = np.mean([out["test"][p] for p in common_p]) if common_p else None
    delta_bacc = round(mean_test - mean_ar, 2) if common_p else None
    # 부호검정: delta>0 품목 수 이항 p (양측, p0=0.5)
    kpos = sum(1 for p in common_p if deltas[p] > 0); ntot = len(common_p)
    from math import comb
    def binom_two_sided(k, n, p=0.5):
        probs = [comb(n, j) * p**j * (1-p)**(n-j) for j in range(n+1)]
        obs = probs[k]; return round(sum(pr for pr in probs if pr <= obs + 1e-12), 4)
    sign_p = binom_two_sided(kpos, ntot) if ntot else None
    # 부트스트랩 CI on mean delta
    dvals = np.array([deltas[p] for p in common_p], float)
    if len(dvals) > 1:
        boot = np.array([dvals[rng.integers(0, len(dvals), len(dvals))].mean() for _ in range(BOOT)])
        ci = [round(np.percentile(boot, 2.5), 2), round(np.percentile(boot, 97.5), 2)]
    else:
        ci = None
    # 대조군 delta
    cp_pc = sorted(set(out["base"]) & set(out["pcplus"]))
    cp_nc = sorted(set(out["base"]) & set(out["nc"]))
    pc_delta = round(np.mean([out["pcplus"][p] - out["base"][p] for p in cp_pc]), 2) if cp_pc else None
    nc_delta = round(np.mean([out["nc"][p] - out["base"][p] for p in cp_nc]), 2) if cp_nc else None
    return {
        "n_products": ntot, "mean_bacc_ar": round(float(mean_ar), 2) if mean_ar else None,
        "mean_bacc_test": round(float(mean_test), 2) if mean_test else None,
        "delta_bacc": delta_bacc, "gate_pass": (delta_bacc is not None and delta_bacc >= GATE),
        "k_products_delta_pos": kpos, "sign_test_p": sign_p, "delta_ci95": ci,
        "PCplus_delta": pc_delta, "NC_delta": nc_delta,
        "per_product_delta": deltas,
        "per_product_bacc": {"ar": out["base"], "test": out["test"]},
    }

print("실행: surge/drop balanced accuracy 백테스트 (개정2)...", flush=True)
res = {"surge": run_mode("surge"), "drop": run_mode("drop")}
controls_ok = all(
    res[m]["PCplus_delta"] is not None and res[m]["PCplus_delta"] >= GATE
    and res[m]["NC_delta"] is not None and res[m]["NC_delta"] < GATE
    for m in ("surge", "drop"))
gate_pass = res["surge"]["gate_pass"] and res["drop"]["gate_pass"]
verdict = "VALID PASS(H1)" if (gate_pass and controls_ok) else (
    "VALID FAIL(H0 유지)" if controls_ok else "INVALID(대조군 실패)")
out = {"prereg": PREREG, "PT": PT, "gate_pp": GATE, "surge_dummy_quantile": SURGE_Q,
       "controls_ok": controls_ok, "gate_pass": gate_pass, "verdict": verdict,
       "metric": "balanced_accuracy (0.5*(TPR+TNR)) on raw next-day surge/drop events, class-weighted logit, expanding train-only",
       "surge": res["surge"], "drop": res["drop"]}
def _np(o):  # numpy 타입 → 파이썬 (직렬화용, 로직 불변)
    if hasattr(o, "item"): return o.item()
    raise TypeError(str(type(o)))
json.dump(out, open(OUTJSON, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=_np)
for m in ("surge", "drop"):
    r = res[m]
    print(f"[{m}] AR bacc={r['mean_bacc_ar']} test bacc={r['mean_bacc_test']} Δ={r['delta_bacc']}pp "
          f"(gate +{GATE}, pass={r['gate_pass']}) | 부호 {r['k_products_delta_pos']}/{r['n_products']} p={r['sign_test_p']} CI95={r['delta_ci95']} "
          f"| PC+Δ={r['PCplus_delta']} NCΔ={r['NC_delta']}", flush=True)
print(f"controls_ok={controls_ok} gate_pass={gate_pass} → {verdict}", flush=True)
print(f"→ {OUTJSON}", flush=True)
