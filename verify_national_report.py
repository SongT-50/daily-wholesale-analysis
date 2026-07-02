# -*- coding: utf-8 -*-
"""전국 리포트 자료 검증 — 3중 대조.
① 독립 파이프라인 대조: settlement_report.load_day/aggregate(정산메일과 동일·검증된 로직)로
   대전 4법인 값을 따로 계산 → 내 리포트 원본 집계와 1:1 일치 확인.
② HTML 재현성(Gate3): 생성된 HTML에 박힌 숫자 ↔ 원본 JSON 재집계 대조.
③ 내부 정합: 시장 합 = 법인 합 = 부류 합, total_collected 필드 = 실제 items 수.
"""
import json, os, re, sys
from datetime import date
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
DATE = "2026-06-27"
S = lambda x: str(x) if x is not None else ""
OK = "\033[92mOK\033[0m"; NG = "\033[91mNG\033[0m"
def line(name, ok, detail=""):
    print(f"  [{'OK' if ok else 'NG'}] {name}{'  '+detail if detail else ''}")
    return ok

def main():
    fpath = os.path.join(DATA, f"auction_{DATE}.json")
    raw = json.load(open(fpath, encoding="utf-8"))

    # ---- 원본 완전 독립 재집계 (build_national과 무관하게 여기서 다시) ----
    corp = defaultdict(lambda: {"qty":0.0,"amt":0.0,"n":0,"market":""})
    market = defaultdict(lambda: {"qty":0.0,"amt":0.0})
    cat = defaultdict(lambda: {"amt":0.0})
    n_items = 0
    daejeon_raw = defaultdict(lambda: {"qty":0.0,"amt":0.0})  # corp_code 기준
    for code, m in raw["markets"].items():
        nm = m.get("market_name","")
        for it in m.get("items", []):
            n_items += 1
            cn = S(it.get("corp_name")); cc = S(it.get("corp_code"))
            q = it.get("total_qty") or 0; a = it.get("total_amount") or 0
            corp[cn]["qty"]+=q; corp[cn]["amt"]+=a; corp[cn]["n"]+=1; corp[cn]["market"]=nm
            market[nm]["qty"]+=q; market[nm]["amt"]+=a
            cat[S(it.get("category"))]["amt"]+=a
            if code in ("250001","250003"):
                daejeon_raw[cc]["qty"]+=q; daejeon_raw[cc]["amt"]+=a
    tot_a = sum(v["amt"] for v in corp.values()); tot_q = sum(v["qty"] for v in corp.values())

    print(f"\n=== 검증 대상: {DATE} 전국 도매시장 리포트 ===\n")
    passed = []

    # ===== ① 독립 파이프라인 대조 (정산메일 로직) =====
    print("① 독립 파이프라인 대조 — settlement_report(정산메일과 동일 로직)로 대전 4법인 재계산")
    sys.path.insert(0, BASE)
    import settlement_report as sr
    y,mn,d = map(int, DATE.split("-"))
    records, _present = sr.load_day(date(y,mn,d))
    agg = sr.aggregate(records)   # {corp_code: {qty_kg, amount, count}}
    for cc in sr.CORP_ORDER:
        short = sr.CORP_SHORT[cc]
        sq = agg[cc]["qty_kg"]; sa = agg[cc]["amount"]
        rq = daejeon_raw[cc]["qty"]; ra = daejeon_raw[cc]["amt"]
        ok = abs(sq-rq) < 1 and abs(sa-ra) < 1
        passed.append(line(f"{short}", ok, f"정산={sq/1000:,.1f}톤/{sa/1e8:.2f}억  vs  리포트원본={rq/1000:,.1f}톤/{ra/1e8:.2f}억"))

    # ===== ② HTML 재현성 (Gate 3) =====
    print("\n② HTML 재현성 — 생성된 HTML에 박힌 숫자 ↔ 원본 재집계")
    hdir = os.path.abspath(os.path.join(BASE,"..","presentations","national-wholesale-daily"))
    hp = os.path.join(hdir, f"WI-인텔리전스_전국도매시장_거래현황_{DATE}.html")
    html = open(hp, encoding="utf-8").read()
    # 총 금액(억) — kpi v 첫 값
    def find_num(pat):
        m = re.search(pat, html)
        return m.group(1) if m else None
    html_amt = find_num(r'총 거래금액</div><div class="v">([\d,\.]+)<i>억원')
    html_ton = find_num(r'총 거래물량</div><div class="v">([\d,]+)<i>톤')
    html_mkt = find_num(r'거래 시장</div><div class="v">(\d+)<i>곳')
    html_corp = find_num(r'법인·공판장</div><div class="v">(\d+)<i>곳')
    exp_amt = f"{tot_a/1e8:,.1f}"; exp_ton = f"{tot_q/1000:,.0f}"
    passed.append(line("총 거래금액", html_amt==exp_amt, f"HTML={html_amt}억  데이터={exp_amt}억"))
    passed.append(line("총 거래물량", html_ton==exp_ton, f"HTML={html_ton}톤  데이터={exp_ton}톤"))
    passed.append(line("거래 시장 수", html_mkt==str(len(market)), f"HTML={html_mkt}  데이터={len(market)}"))
    passed.append(line("법인·공판장 수", html_corp==str(len(corp)), f"HTML={html_corp}  데이터={len(corp)}"))
    # 지도 버블 = 좌표 있는 시장 수 / 법인 행 = 법인 수
    n_circle = len(re.findall(r'<circle', html))
    n_frow = len(re.findall(r'class="frow', html))
    passed.append(line("법인 랭킹 행 수 = 법인 수", n_frow==len(corp), f"HTML행={n_frow}  법인={len(corp)}"))

    # ===== ③ 내부 정합 =====
    print("\n③ 내부 정합 — 집계 축끼리·원본 필드")
    m_a = sum(v["amt"] for v in market.values()); c_a = sum(v["amt"] for v in cat.values())
    passed.append(line("시장합 = 법인합 (금액)", abs(m_a-tot_a)<1, f"{m_a/1e8:.1f}억 = {tot_a/1e8:.1f}억"))
    passed.append(line("부류합 = 전체합 (금액)", abs(c_a-tot_a)<1, f"{c_a/1e8:.1f}억 = {tot_a/1e8:.1f}억"))
    tc = raw.get("total_collected")
    passed.append(line("total_collected 필드 = 실제 items 수", tc==n_items, f"필드={tc}  실제={n_items}"))

    print(f"\n=== 결과: {sum(passed)}/{len(passed)} 통과 ===")
    print("[전부 통과]" if all(passed) else "[불일치 있음 — 위 NG 확인]")
    sys.exit(0 if all(passed) else 1)

if __name__ == "__main__":
    main()
