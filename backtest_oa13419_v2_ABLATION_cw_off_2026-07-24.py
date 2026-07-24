# OA-13419 v2 ROBUSTNESS ABLATION — class-weight OFF
# ★ 사후 민감도 분석(결과 본 뒤). 정본 판정 = 사전등록된 class-weighted 버전(RESULT_oa13419_v2, VALID FAIL) 불변.
# ★ 목적 = "VALID FAIL이 class-weight 선택의 산물인가"를 실측(G-E measurement-analyzer needs-more 갭 닫기).
# ★ 원본 backtest_oa13419_v2_2026-07-24.py와 유일 차이 = 119행 sw(클래스가중)를 균등(np.ones)으로 = unweighted logit.
#   나머지(라벨·특징·expanding·대조군·게이트·seed) 전부 동일. 출력은 별도 파일(원본 RESULT 안 덮어씀).
# anti-fishing: 이 결과로 정본 판정을 뒤집지 않는다. "판정이 class-weight에 견고한가"만 본다.
import csv, json
import numpy as np
from collections import defaultdict
rng = np.random.default_rng(20260724)  # 원본과 동일 seed = NC 순열 동일 = 공정 비교

BASE = "C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/"
ARR = BASE + "oa13419_garak_arrival.csv"; PANEL = BASE + "supply_price_panel.csv"
OUTJSON = BASE + "RESULT_oa13419_v2_ABLATION_cw_off_2026-07-24.json"  # ★ 별도 파일
PREREG = "PREREG_oa13419_v2_metric_realign_2026-07-24.md (ABLATION cw-off, 사후 robustness)"
P18 = ['감귤','감자','깻잎','당근','대파','딸기','마늘','무','배추','사과','상추','시금치','양배추','양파','오이','토마토','파프리카','포도']
PT = 5.0
GATE = 5.0
SURGE_Q = 90.0
MAX_GAP = 4; WARMUP = 504; BLOCK = 20
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
    pa = arr.get(prod, {}); pp = price.get(prod, {})
    common = sorted(set(pa) & set(pp))
    if len(common) < WARMUP + BLOCK + 5: return None
    N = len(common)
    pv = np.array([pp[d] for d in common]); av = np.array([pa[d] for d in common])
    mon = np.array([int(d[5:7]) for d in common])
    pct = np.full(N, np.nan)
    for k in range(1, N):
        if dgap(common[k], common[k-1]) <= MAX_GAP and pv[k-1] > 0:
            pct[k] = (pv[k] - pv[k-1]) / pv[k-1] * 100.0
    if arm == "nc":
        av = av[rng.permutation(N)]
    TP = FN = TN = FP = 0
    i = WARMUP
    while i < N - 1:
        tr = np.zeros(N, bool); tr[:i] = True
        zp = zseries(pv, mon, tr); za = zseries(av, mon, tr)
        za_tr = za[tr]; za_tr = za_tr[~np.isnan(za_tr)]
        qz = np.percentile(za_tr, SURGE_Q) if len(za_tr) >= 10 else np.inf

        def feat(k):
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
        # ★★ ABLATION: class weight OFF = 균등가중 (원본은 sw=역빈도). unweighted logit.
        sw = np.ones(len(Ytr))
        model = logit_fit(Xtr, Ytr, sw)
        for k in range(i, min(i + BLOCK, N - 1)):
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
        return None
    tpr = TP / (TP + FN); tnr = TN / (TN + FP)
    return round(100 * 0.5 * (tpr + tnr), 2)

def run_mode(mode):
    out = {"base": {}, "test": {}, "pcplus": {}, "nc": {}}
    for prod in P18:
        for arm in ("base", "test", "pcplus", "nc"):
            b = measure_product(prod, mode, arm)
            if b is not None:
                out[arm][prod] = b
    common_p = sorted(set(out["base"]) & set(out["test"]))
    deltas = {p: round(out["test"][p] - out["base"][p], 2) for p in common_p}
    mean_ar = np.mean([out["base"][p] for p in common_p]) if common_p else None
    mean_test = np.mean([out["test"][p] for p in common_p]) if common_p else None
    delta_bacc = round(mean_test - mean_ar, 2) if common_p else None
    kpos = sum(1 for p in common_p if deltas[p] > 0); ntot = len(common_p)
    from math import comb
    def binom_two_sided(k, n, p=0.5):
        probs = [comb(n, j) * p**j * (1-p)**(n-j) for j in range(n+1)]
        obs = probs[k]; return round(sum(pr for pr in probs if pr <= obs + 1e-12), 4)
    sign_p = binom_two_sided(kpos, ntot) if ntot else None
    dvals = np.array([deltas[p] for p in common_p], float)
    if len(dvals) > 1:
        boot = np.array([dvals[rng.integers(0, len(dvals), len(dvals))].mean() for _ in range(BOOT)])
        ci = [round(np.percentile(boot, 2.5), 2), round(np.percentile(boot, 97.5), 2)]
    else:
        ci = None
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

print("실행: ABLATION class-weight OFF (unweighted logit)...", flush=True)
res = {"surge": run_mode("surge"), "drop": run_mode("drop")}
controls_ok = all(
    res[m]["PCplus_delta"] is not None and res[m]["PCplus_delta"] >= GATE
    and res[m]["NC_delta"] is not None and res[m]["NC_delta"] < GATE
    for m in ("surge", "drop"))
gate_pass = res["surge"]["gate_pass"] and res["drop"]["gate_pass"]
verdict = "PASS(H1) [ablation]" if (gate_pass and controls_ok) else (
    "FAIL(H0) [ablation]" if controls_ok else "INVALID(대조군 실패) [ablation]")
out = {"prereg": PREREG, "ablation": "class_weight_OFF (sw=ones, unweighted logit)",
       "PT": PT, "gate_pp": GATE, "surge_dummy_quantile": SURGE_Q,
       "controls_ok": controls_ok, "gate_pass": gate_pass, "verdict": verdict,
       "metric": "balanced_accuracy on raw next-day surge/drop, UNWEIGHTED logit, expanding train-only",
       "surge": res["surge"], "drop": res["drop"]}
def _np(o):
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
