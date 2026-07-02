# -*- coding: utf-8 -*-
"""전국 농수산물 도매시장 하루 거래현황 — 1장짜리 창의적 리포트.
정산자료는 3~4일 지연 확정 → --date 로 확정일 지정(기본 2026-06-27).
법인/공판장 단위(83개 등록, 당일 거래분) + 시장 지도 버블 + 규모 티어 + 부류 요약.
사용: python build_national_report.py --date 2026-06-27
"""
import json, os, sys, math, argparse
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
# 데이터 경로: AUCTION_ARCHIVE_DIR(Actions=data) 우선, 상대경로면 BASE 기준, 없으면 BASE/data
_env = os.getenv("AUCTION_ARCHIVE_DIR")
if _env and not os.path.isabs(_env):
    _env = os.path.join(BASE, _env)
DATA = _env if _env and os.path.isdir(_env) else os.path.join(BASE, "data")
S = lambda x: str(x) if x is not None else ""

# 32개 도매시장 대략 좌표 (위도, 경도) — 지도상 위치감용
MARKET_XY = {
    "서울가락": (37.492, 127.118), "서울강서": (37.561, 126.813), "구리": (37.598, 127.140),
    "인천남촌": (37.418, 126.735), "인천삼산": (37.523, 126.744), "수원": (37.264, 127.000),
    "안양": (37.401, 126.922), "안산": (37.317, 126.837), "대전오정": (36.365, 127.417),
    "대전노은": (36.378, 127.311), "천안": (36.815, 127.113), "청주": (36.639, 127.467),
    "충주": (36.991, 127.926), "대구북부": (35.906, 128.545), "구미": (36.119, 128.344),
    "포항": (36.019, 129.343), "안동": (36.568, 128.729), "부산엄궁": (35.152, 128.972),
    "부산반여": (35.213, 129.113), "울산": (35.539, 129.331), "창원팔용": (35.243, 128.611),
    "창원내서": (35.257, 128.545), "진주": (35.180, 128.108), "광주서부": (35.160, 126.858),
    "광주각화": (35.174, 126.933), "전주": (35.856, 127.107), "익산": (35.948, 126.958),
    "정읍": (35.570, 126.856), "순천": (34.951, 127.487), "강릉": (37.755, 128.896),
    "원주": (37.342, 127.920), "춘천": (37.881, 127.730),
}
# 우리 법인 강조
OURS = {"대전중앙청과㈜", "대전원협노은(공)"}

# 지도 SVG 파라미터
MW, MH = 380, 500
LON0, LON1, LAT0, LAT1 = 125.5, 129.9, 34.2, 38.75
def proj(lat, lon):
    x = (lon - LON0) / (LON1 - LON0) * MW
    y = (LAT1 - lat) / (LAT1 - LAT0) * MH
    return x, y

# 남한 윤곽 근사 (위도, 경도) 시계방향
KR_OUTLINE = [
    (37.75,126.55),(37.05,126.55),(36.75,126.12),(36.0,126.5),(35.6,126.45),
    (35.1,126.38),(34.55,126.28),(34.3,126.55),(34.62,127.28),(34.75,127.72),
    (34.9,128.4),(35.08,128.62),(35.1,129.05),(35.5,129.38),(36.05,129.45),
    (36.9,129.42),(37.55,129.1),(38.05,128.72),(38.5,128.42),(38.35,127.4),
    (38.05,127.0),(37.9,126.68),(37.75,126.55),
]

def load(date):
    f = os.path.join(DATA, f"auction_{date}.json")
    if not os.path.exists(f):
        sys.exit(f"[X] {f} 없음 — 정산 확정일 확인")
    return json.load(open(f, encoding="utf-8"))

def aggregate(data):
    corp = defaultdict(lambda: {"qty":0.0,"amt":0.0,"items":0,"market":"","tt":defaultdict(float)})
    market = defaultdict(lambda: {"qty":0.0,"amt":0.0,"corps":set()})
    cat = defaultdict(lambda: {"qty":0.0,"amt":0.0})
    tt = defaultdict(lambda: {"amt":0.0})
    for code, m in data["markets"].items():
        nm = m.get("market_name","")
        for it in m.get("items", []):
            cn = S(it.get("corp_name")); q = it.get("total_qty") or 0; a = it.get("total_amount") or 0
            corp[cn]["qty"]+=q; corp[cn]["amt"]+=a; corp[cn]["items"]+=1; corp[cn]["market"]=nm
            market[nm]["qty"]+=q; market[nm]["amt"]+=a; market[nm]["corps"].add(cn)
            cat[S(it.get("category"))]["qty"]+=q; cat[S(it.get("category"))]["amt"]+=a
            tt[S(it.get("trade_type"))]["amt"]+=a
    return corp, market, cat, tt

def fmt_eok(a): return f"{a/1e8:,.1f}"
def fmt_ton(q): return f"{q/1000:,.0f}"

def tier_of(amt):
    if amt >= 10e8: return 1
    if amt >= 3e8:  return 2
    return 3
TIER = {1:("1군","대형 거점","#1d4ed8"),2:("2군","중형","#0891b2"),3:("3군","소형·지역","#64748b")}

def build(date):
    data = load(date)
    corp, market, cat, tt = aggregate(data)
    tot_a = sum(v["amt"] for v in corp.values()); tot_q = sum(v["qty"] for v in corp.values())
    n_corp = len(corp); n_market = len(market)

    # 요일
    import datetime
    dt = datetime.date.fromisoformat(date)
    wd = "월화수목금토일"[dt.weekday()]

    # 지도 윤곽 path
    pts = [proj(la,lo) for la,lo in KR_OUTLINE]
    outline = "M " + " L ".join(f"{x:.1f},{y:.1f}" for x,y in pts) + " Z"

    # 시장 버블
    max_ma = max(v["amt"] for v in market.values())
    bubbles = []
    for nm, v in sorted(market.items(), key=lambda x:-x[1]["amt"]):
        if nm not in MARKET_XY: continue
        la, lo = MARKET_XY[nm]; x, y = proj(la, lo)
        r = 5 + math.sqrt(v["amt"]/max_ma) * 30
        t = tier_of(v["amt"]); col = TIER[t][2]
        bubbles.append((nm, x, y, r, v["amt"], v["qty"], col, t))
    # 상위 라벨
    top_lab = sorted(bubbles, key=lambda b:-b[4])[:9]
    lab_names = {b[0] for b in top_lab}

    circles = ""
    for nm,x,y,r,a,q,col,t in bubbles:
        ours = "1" if any(c in OURS for c in market[nm]["corps"]) else "0"
        stroke = "#f59e0b" if ours=="1" else "#fff"
        sw = "2.5" if ours=="1" else "1"
        circles += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{col}" fill-opacity="0.62" stroke="{stroke}" stroke-width="{sw}"><title>{nm} · {fmt_eok(a)}억 · {fmt_ton(q)}톤</title></circle>\n'
    labels = ""
    for nm,x,y,r,a,q,col,t in bubbles:
        if nm in lab_names:
            dy = -r-3 if y>60 else r+11
            labels += f'<text x="{x:.1f}" y="{y+dy:.1f}" text-anchor="middle" class="mlab">{nm}</text>\n'
            labels += f'<text x="{x:.1f}" y="{y+dy+11:.1f}" text-anchor="middle" class="mval">{fmt_eok(a)}억</text>\n'

    # 티어별 요약
    tier_sum = defaultdict(lambda: {"amt":0.0,"qty":0.0,"n":0})
    for nm,v in market.items():
        t = tier_of(v["amt"]); tier_sum[t]["amt"]+=v["amt"]; tier_sum[t]["qty"]+=v["qty"]; tier_sum[t]["n"]+=1
    tier_cards = ""
    for t in (1,2,3):
        s = tier_sum[t]; nm,desc,col = TIER[t]
        share = s["amt"]/tot_a*100 if tot_a else 0
        tier_cards += f'''<div class="tcard" style="border-top:3px solid {col}">
          <div class="tname" style="color:{col}">{nm} <span>{desc}</span></div>
          <div class="tbig">{fmt_eok(s['amt'])}<i>억</i></div>
          <div class="tsub">시장 {s['n']}곳 · {fmt_ton(s['qty'])}톤 · 금액비중 {share:.0f}%</div>
        </div>'''

    # 부류 요약 (상위 8)
    cat_rows = ""
    max_ca = max((v["amt"] for v in cat.values()), default=1)
    for cname, v in sorted(cat.items(), key=lambda x:-x[1]["amt"])[:8]:
        w = v["amt"]/max_ca*100
        cat_rows += f'''<div class="crow"><span class="cn">{cname or '기타'}</span>
          <span class="cbarwrap"><span class="cbar" style="width:{w:.0f}%"></span></span>
          <span class="cv">{fmt_eok(v['amt'])}억</span></div>'''

    # 법인 랭킹 — 시장별 그룹, 금액순
    max_corp = max(v["amt"] for v in corp.values())
    mkt_order = sorted(market.items(), key=lambda x:-x[1]["amt"])
    corp_blocks = ""
    rank = 0
    for nm, mv in mkt_order:
        members = sorted([(cn,cv) for cn,cv in corp.items() if cv["market"]==nm], key=lambda x:-x[1]["amt"])
        t = tier_of(mv["amt"]); col = TIER[t][2]
        rows = ""
        for cn, cv in members:
            rank += 1
            w = cv["amt"]/max_corp*100
            ours = " ours" if cn in OURS else ""
            rows += f'''<div class="frow{ours}">
              <span class="fname">{cn}</span>
              <span class="fbarwrap"><span class="fbar" style="width:{max(w,1):.1f}%;background:{col}"></span></span>
              <span class="famt">{fmt_eok(cv['amt'])}</span></div>'''
        corp_blocks += f'''<div class="mblock">
          <div class="mhead"><span class="mdot" style="background:{col}"></span>{nm}
            <span class="mmeta">{len(members)}개사 · {fmt_eok(mv['amt'])}억 · {fmt_ton(mv['qty'])}톤</span></div>
          {rows}</div>'''

    # 온라인/거래유형 pill
    online = tt.get("전자거래",{}).get("amt",0)+tt.get("전자거래(팩스형)",{}).get("amt",0)
    auc = tt.get("경매",{}).get("amt",0); jga = tt.get("정가수의",{}).get("amt",0)+tt.get("정가수의(예약형)",{}).get("amt",0)
    def pct(x): return x/tot_a*100 if tot_a else 0

    html = f'''<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>전국 도매시장 거래현황 {date}</title>
<link href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css" rel="stylesheet">
<style>
:root{{--bg:#eef1f6;--card:#fff;--ink:#0f1b2d;--sub:#5c6b82;--line:#e3e8f0;--accent:#1d4ed8;--amber:#f59e0b}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Pretendard','Noto Sans KR',sans-serif;background:var(--bg);color:var(--ink);
  -webkit-print-color-adjust:exact;print-color-adjust:exact}}
.page{{max-width:1180px;margin:0 auto;padding:26px 30px 40px}}
.top{{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;
  border-bottom:2.5px solid var(--ink);padding-bottom:14px;margin-bottom:18px}}
.brand{{display:flex;align-items:center;gap:7px;font-size:13px;font-weight:800;color:var(--accent);
  letter-spacing:-.2px;margin-bottom:7px}}
.brand .bmark{{background:linear-gradient(135deg,#1d4ed8,#22d3ee);color:#fff;font-weight:800;
  font-size:12px;padding:2px 7px;border-radius:6px;letter-spacing:.5px}}
.brand .bdot{{color:var(--line)}}
.title{{font-size:27px;font-weight:800;letter-spacing:-.5px}}
.title b{{color:var(--accent)}}
.subt{{font-size:13px;color:var(--sub);margin-top:5px}}
.datebox{{text-align:right}}
.datebig{{font-size:22px;font-weight:800;letter-spacing:-.5px}}
.datesub{{font-size:11.5px;color:var(--sub);margin-top:3px}}
.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px}}
.kpi{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px 17px;
  box-shadow:0 1px 3px rgba(15,27,45,.04)}}
.kpi .k{{font-size:11.5px;color:var(--sub);font-weight:600;letter-spacing:.2px}}
.kpi .v{{font-size:28px;font-weight:800;letter-spacing:-1px;margin-top:3px}}
.kpi .v i{{font-size:15px;font-weight:700;color:var(--sub);font-style:normal;margin-left:2px}}
.kpi .d{{font-size:11px;color:var(--sub);margin-top:2px}}
.grid{{display:grid;grid-template-columns:400px 1fr;gap:18px;margin-bottom:18px}}
.panel{{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px 18px;
  box-shadow:0 1px 3px rgba(15,27,45,.04)}}
.ph{{font-size:14px;font-weight:800;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}}
.ph small{{font-size:11px;font-weight:600;color:var(--sub)}}
svg .land{{fill:#f5f8fd;stroke:#c6d2e4;stroke-width:1.2}}
svg .mlab{{font:700 9.5px 'Pretendard';fill:#0f1b2d}}
svg .mval{{font:700 8.5px 'Pretendard';fill:#1d4ed8}}
.leg{{display:flex;gap:14px;margin-top:8px;font-size:11px;color:var(--sub);flex-wrap:wrap}}
.leg i{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:4px;vertical-align:-1px}}
.tiers{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px}}
.tcard{{background:#fafcff;border:1px solid var(--line);border-radius:12px;padding:13px 15px}}
.tname{{font-size:14px;font-weight:800}}
.tname span{{font-size:11px;font-weight:600;color:var(--sub);margin-left:4px}}
.tbig{{font-size:26px;font-weight:800;letter-spacing:-1px;margin-top:4px}}
.tbig i{{font-size:13px;color:var(--sub);font-style:normal;margin-left:2px}}
.tsub{{font-size:11px;color:var(--sub);margin-top:3px}}
.crow{{display:flex;align-items:center;gap:9px;margin:6px 0;font-size:12px}}
.crow .cn{{width:78px;color:var(--sub);font-weight:600;flex:none}}
.cbarwrap{{flex:1;background:#eef2f8;border-radius:5px;height:11px;overflow:hidden}}
.cbar{{display:block;height:100%;background:linear-gradient(90deg,#2563eb,#22d3ee);border-radius:5px}}
.crow .cv{{width:52px;text-align:right;font-weight:700;flex:none}}
.online{{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}}
.opill{{font-size:11.5px;background:#f1f5fb;border:1px solid var(--line);border-radius:20px;padding:5px 12px;font-weight:600}}
.opill b{{color:var(--accent)}}
.ranks .ph{{margin-bottom:12px}}
.mcols{{column-count:3;column-gap:20px}}
.mblock{{break-inside:avoid;margin-bottom:13px}}
.mhead{{font-size:12.5px;font-weight:800;padding-bottom:4px;border-bottom:1.5px solid var(--line);
  margin-bottom:5px;display:flex;align-items:center;gap:5px}}
.mdot{{width:8px;height:8px;border-radius:50%;flex:none}}
.mmeta{{font-size:10px;font-weight:600;color:var(--sub);margin-left:auto}}
.frow{{display:flex;align-items:center;gap:6px;font-size:11px;margin:2.5px 0}}
.fname{{width:96px;flex:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.fbarwrap{{flex:1;background:#f0f3f8;border-radius:4px;height:8px;overflow:hidden}}
.fbar{{display:block;height:100%;border-radius:4px;opacity:.85}}
.famt{{width:34px;text-align:right;font-weight:700;flex:none}}
.frow.ours .fname{{color:#b45309;font-weight:800}}
.frow.ours{{background:#fff7e8;border-radius:5px;padding:1px 4px;margin-left:-4px}}
.foot{{margin-top:20px;font-size:11px;color:var(--sub);text-align:center;line-height:1.7;
  border-top:1px solid var(--line);padding-top:12px}}
@media print{{body{{background:#fff}} .page{{max-width:100%;padding:10px}}}}
</style></head><body><div class="page">

<div class="top">
  <div>
    <div class="brand"><span class="bmark">WI</span>인텔리전스 <span class="bdot">·</span> Wholesale Intelligence</div>
    <div class="title">전국 농수산물 <b>도매시장</b> 거래현황</div>
    <div class="subt">32개 도매시장 · 법인·공판장 {n_corp}곳 통합 · 정산 확정분(katSale) 기준</div></div>
  <div class="datebox"><div class="datebig">{dt.year}. {dt.month}. {dt.day} <span style="font-size:15px">({wd})</span></div>
    <div class="datesub">정산자료는 거래 3~4일 후 확정 · 확정 마감일 기준</div></div>
</div>

<div class="kpis">
  <div class="kpi"><div class="k">총 거래금액</div><div class="v">{fmt_eok(tot_a)}<i>억원</i></div><div class="d">전국 합산</div></div>
  <div class="kpi"><div class="k">총 거래물량</div><div class="v">{fmt_ton(tot_q)}<i>톤</i></div><div class="d">{tot_q:,.0f} kg</div></div>
  <div class="kpi"><div class="k">거래 시장</div><div class="v">{n_market}<i>곳</i></div><div class="d">전국 도매시장</div></div>
  <div class="kpi"><div class="k">법인·공판장</div><div class="v">{n_corp}<i>곳</i></div><div class="d">당일 거래 발생</div></div>
</div>

<div class="grid">
  <div class="panel">
    <div class="ph">전국 거래 지도 <small>버블=거래금액 규모</small></div>
    <svg viewBox="0 0 {MW} {MH}" width="100%" style="max-height:500px">
      <path class="land" d="{outline}"/>
      {circles}{labels}
    </svg>
    <div class="leg">
      <span><i style="background:#1d4ed8"></i>1군 대형</span>
      <span><i style="background:#0891b2"></i>2군 중형</span>
      <span><i style="background:#64748b"></i>3군 소형</span>
      <span><i style="background:#f59e0b"></i>대전(우리 법인)</span>
    </div>
  </div>

  <div>
    <div class="tiers">{tier_cards}</div>
    <div class="panel">
      <div class="ph">부류별 거래금액 <small>상위 8</small></div>
      {cat_rows}
      <div class="online">
        <span class="opill">경매 <b>{pct(auc):.0f}%</b></span>
        <span class="opill">정가수의 <b>{pct(jga):.0f}%</b></span>
        <span class="opill">전자·온라인 <b>{pct(online):.1f}%</b></span>
      </div>
    </div>
  </div>
</div>

<div class="panel ranks">
  <div class="ph">시장별 법인·공판장 랭킹 <small>전 {n_corp}곳 · 막대=거래금액(억) · 노랑=우리 법인</small></div>
  <div class="mcols">{corp_blocks}</div>
</div>

<div class="foot">
  <b>WI-인텔리전스 (Wholesale Intelligence)</b> · 자료: 전국 농수산물도매시장 통합 정산데이터(aT katSale API, data.go.kr)<br>
  ※ 정산자료는 거래일 3~4일 후 확정되므로 본 리포트는 <b>확정 마감일({date})</b> 기준입니다.
</div>
</div></body></html>'''

    outdir = os.path.abspath(os.path.join(BASE, "..", "presentations", "national-wholesale-daily"))
    os.makedirs(outdir, exist_ok=True)
    # 날짜 넣은 파일명 → 매일 누적, 메일 첨부 시 겹치지 않음
    outpath = os.path.join(outdir, f"WI-인텔리전스_전국도매시장_거래현황_{date}.html")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(html)
    print("[OK]", outpath)
    print(f"     전국 {fmt_eok(tot_a)}억 / {fmt_ton(tot_q)}톤 / 시장 {n_market} / 법인 {n_corp}")
    return outpath

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-06-27")
    a = ap.parse_args()
    build(a.date)
