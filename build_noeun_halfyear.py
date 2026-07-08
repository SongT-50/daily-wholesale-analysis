# -*- coding: utf-8 -*-
"""노은도매시장 거래현황 — 2026년 상반기(1~6월) 중앙청과 vs 원협노은 한 장 보고서.

6월 누계 보고서(build_noeun_report.py)와 같은 양식·경매사 분류(AUCTION_BLOCKS/agg_auctioneer 재사용).

★상반기 데이터 정정 (data-to-claim 정직성):
 - 원협 4·5월 = aT유통공사 '도매시장 거래현황' .xls 사용
   (우리 data.go.kr 아카이브가 계통출하 이중집계로 189.9/150.9억 과대 → 122.5/125.7억으로 교체.
    단 aT유통공사 자료는 경매+정가수의만 = 온라인 전자거래 제외 → 노은 관리사업소 공식보다 소폭 낮음).
 - 중앙 2/24 소실 5.08억/244.7톤 = aT 원천 부재 → 회사 정산 자료로 총계 보정
   (경매사별 표는 aT 원천값 그대로 두고 총계·주석으로 대사 = 소계+보정 = 공식 상반기).
 - 검증: 중앙 835.6억/38,241톤 = 회사 월계표 공식(835.7/38,241) 정확 일치.
"""
import os, sys, json, re
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import settlement_report as sr
import build_noeun_report as bn

J, W = "25000301", "25000302"
BURYU = {'두류':'03','잡곡류':'04','서류':'05','과실류':'06','수실류':'07','과일과채류':'08',
         '과채류':'09','엽경채류':'10','근채류':'11','조미채소류':'12','양채류':'13',
         '산채류':'14','특용작물류':'16','버섯류':'17','약용작물류':'19','농림가공':'91'}
DL = os.path.join(os.path.expanduser("~"), "Downloads")
XLS = {4: os.path.join(DL, "도매시장 거래현황 2026-04-01-2026-04-30.xls"),
       5: os.path.join(DL, "도매시장 거래현황 2026-05-01-2026-05-31.xls")}
CORR = json.load(open(os.path.join(HERE, "corrections", "missing_corrections.json"), encoding="utf-8"))
MDAYS = CORR["missing_days"]
# 관리사업소 노은-오정 현황판(당월 시트 2026계 r9) 공식 월별 금액(억) = (중앙, 원협). 6월 미제공.
# 출처: Downloads 노은-오정시장 현황판(2026년N월).xlsx 1~5월, 당월 시트 K9/M9(백만원→억).
OFFICIAL = {1: (143.2, 131.7), 2: (166.4, 164.6), 3: (132.1, 122.5),
            4: (128.8, 126.2), 5: (133.7, 126.8)}


def xls_recs(mon):
    """aT유통공사 .xls → agg_auctioneer 호환 레코드(원협)."""
    df = pd.read_excel(XLS[mon])
    out = []
    for _, r in df.iterrows():
        cc = BURYU.get(str(r['부류']).strip(), '')
        out.append({'corp_code': W, 'product': str(r['품목']).strip(), 'category_code': cc,
                    'total_qty': float(r['물량(kg)']), 'total_amount': float(r['금액(원)'])})
    return out


def jung_month_corr(year, m):
    """중앙 소실일(회사 정산) 보정액 = (amount, qty)."""
    a = q = 0.0
    for dk, v in MDAYS.items():
        if dk.startswith(f"{year}-") and int(dk[5:7]) == m:
            a += v['amount']; q += v['qty_kg']
    return a, q


def month_totals(year, use_xls_45):
    """월별 (중앙 amt,qty / 원협 amt,qty). 중앙=소실보정 / 원협 4·5월=aT유통공사(2026만)."""
    res = {}
    for m in range(1, 7):
        s = date(year, m, 1); e = date(year, m + 1, 1) - timedelta(days=1)
        recs, _ = sr.load_range(s, e)
        ja = sum(r.get('total_amount', 0) for r in recs if r.get('corp_code') == J)
        jq = sum(r.get('total_qty', 0) for r in recs if r.get('corp_code') == J)
        ca, cq = jung_month_corr(year, m); ja += ca; jq += cq
        if use_xls_45 and m in (4, 5):
            xr = xls_recs(m)
            wa = sum(x['total_amount'] for x in xr); wq = sum(x['total_qty'] for x in xr)
        else:
            wa = sum(r.get('total_amount', 0) for r in recs if r.get('corp_code') == W)
            wq = sum(r.get('total_qty', 0) for r in recs if r.get('corp_code') == W)
        res[m] = (ja, jq, wa, wq)
    return res


def auctioneer_halfyear():
    """경매사별 2026 상반기 item-level (중앙 아카이브 1~6 / 원협 아카이브 1·2·3·6 + xls 4·5)."""
    recs_all = []
    for m in range(1, 7):
        s = date(2026, m, 1); e = date(2026, m + 1, 1) - timedelta(days=1)
        recs, _ = sr.load_range(s, e)
        recs_all += [r for r in recs if r.get('corp_code') == J]
        if m in (4, 5):
            recs_all += xls_recs(m)
        else:
            recs_all += [r for r in recs if r.get('corp_code') == W]
    return bn.agg_auctioneer(recs_all)


# ───────────────────────── 포맷 헬퍼 ─────────────────────────
def eok(a): return f"{a/1e8:.1f}"
def ton(q): return f"{q/1000:,.0f}"
def won(a): return f"{a:,.0f}"
def kg(q): return f"{q:,.0f}"
def pct(a, b): return (a / (a + b) * 100) if (a + b) else 0


def pcls(p):
    if p >= 55: return "p-big"
    if p >= 51: return "p-win"
    if p >= 49.5: return "p-mid"
    return "p-lose"


def prod_group(prods_lb, n=3):
    top = sorted(prods_lb.items(), key=lambda x: -x[1])[:n]
    return "·".join(p for p, _ in top)


def build():
    m26 = month_totals(2026, use_xls_45=True)
    # 누계 (2026 상반기, 중앙 vs 원협만 — 작년 비교 제거: 태은이 7/8 "순전히 올해 비교")
    j26a = sum(v[0] for v in m26.values()); j26q = sum(v[1] for v in m26.values())
    w26a = sum(v[2] for v in m26.values()); w26q = sum(v[3] for v in m26.values())
    amt_share26 = pct(j26a, w26a); qty_share26 = pct(j26q, w26q)
    gap26 = (j26a - w26a) / 1e8

    data, order, prods = auctioneer_halfyear()
    labels = sorted(order, key=lambda x: order[x])
    # ★'나머지'와 '미배정'을 한 행으로 합치고, 실제 품목을 괄호에 나열 (태은이 7/8)
    rows = []
    rq = [0.0, 0.0]; rw = [0.0, 0.0]; rprod = defaultdict(float)
    for lb in labels:
        jq, ja = data[lb][J]; wq, wa = data[lb][W]
        if ja + wa <= 0:
            continue
        cl = bn.clean_label(lb)
        if cl.startswith("나머지") or cl.startswith("미배정"):
            rq[0] += jq; rq[1] += ja; rw[0] += wq; rw[1] += wa
            for p, a in prods[lb].items():
                rprod[p] += a
            continue
        rows.append((cl, prod_group(prods[lb]), jq, ja, wq, wa, pct(ja, wa)))
    if rq[1] + rw[1] > 0:
        top = [p for p, _ in sorted(rprod.items(), key=lambda x: -x[1])]
        plist = "·".join(top[:12]) + (" 등" if len(top) > 12 else "")
        rows.append(("나머지·미배정", plist, rq[0], rq[1], rw[0], rw[1], pct(rq[1], rw[1])))
    sja = sum(r[3] for r in rows); sjq = sum(r[2] for r in rows)
    swa = sum(r[5] for r in rows); swq = sum(r[4] for r in rows)
    corr24 = MDAYS["2026-02-24"]  # 중앙 2/24 보정

    # 월별 추이 행 (재집계 + 관리사업소 공식 병기)
    mrows = ""
    names = {1:"1월",2:"2월",3:"3월",4:"4월",5:"5월",6:"6월"}
    for m in range(1, 7):
        ja, jq, wa, wq = m26[m]
        sh = pct(ja, wa)
        of = OFFICIAL.get(m)
        ojt = f"{of[0]:.1f}" if of else "—"
        owt = f"{of[1]:.1f}" if of else "—"
        # 원협 재집계 vs 관리사업소 공식 차이 0.5억+ 강조(4·5월 온라인 차이)
        wdiff = of and abs((wa / 1e8) - of[1]) >= 0.5
        owstyle = ' style="color:#b45309;font-weight:700"' if wdiff else ''
        src = "aT유통공사" if m in (4, 5) else ("소실보정" if m == 2 else "")
        srctxt = f' <span class="sub">({src})</span>' if src else ""
        cls = "p-win" if sh >= 50 else "p-lose"
        mrows += (f'<tr><td class="lbl">{names[m]}{srctxt}</td>'
                  f'<td class="colj">{eok(ja)}</td><td class="colj offc">{ojt}</td>'
                  f'<td class="colw">{eok(wa)}</td><td class="colw offc"{owstyle}>{owt}</td>'
                  f'<td class="pct {cls}">{sh:.1f}%</td></tr>')

    # 경매사별 표 행
    arows = ""
    for nm, pg, jq, ja, wq, wa, p in rows:
        wrap = ' style="white-space:normal;line-height:1.3"' if nm.startswith("나머지") else ''
        arows += (f'<tr><td class="lbl"{wrap}>{nm} <span class="sub">({pg})</span></td>'
                  f'<td class="colj">{kg(jq)}</td><td class="colj">{won(ja)}</td>'
                  f'<td class="colw">{kg(wq)}</td><td class="colw">{won(wa)}</td>'
                  f'<td class="pct {pcls(p)}"><span class="pnm">{nm}</span>{p:.1f}%</td></tr>')

    HTML = f"""<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>노은도매시장 거래현황 — 중앙청과 vs 원협노은 (2026 상반기)</title>
<style>
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
:root{{--ink:#1a1a1a;--sub:#666;--line:#cfcfcf;--line2:#e8e8e8;
  --jung:#1d4ed8;--jung2:#3b6fe0;--won:#c2622f;--won2:#d98a52;
  --hi:#fff7cc;--good:#15803d;--goodbg:#e9f7ee;--bad:#b91c1c;--badbg:#fdeceb;}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Pretendard',sans-serif;color:var(--ink);background:#e9ecf1;font-size:11.5px;line-height:1.32;-webkit-font-smoothing:antialiased}}
.page{{width:210mm;min-height:297mm;margin:14px auto;background:#fff;padding:8mm 10mm 6mm;box-shadow:0 2px 16px rgba(0,0,0,.18)}}
h1{{font-size:18px;font-weight:800;letter-spacing:-.5px}}
.head{{display:flex;justify-content:space-between;align-items:flex-end;border-bottom:2.5px solid var(--ink);padding-bottom:5px;margin-bottom:8px}}
.head .meta{{text-align:right;color:var(--sub);font-size:11px;line-height:1.5}}
.head .meta b{{color:var(--ink)}}
.legend{{font-size:10.5px;color:var(--sub);margin-top:3px}}
.legend .cj{{color:var(--jung);font-weight:700}}.legend .cw{{color:var(--won);font-weight:700}}
.dash{{border:1.5px solid #c7d6f5;border-radius:9px;padding:8px 12px 9px;margin-bottom:9px;background:linear-gradient(180deg,#f7faff,#fff)}}
.dash-h{{font-size:12.5px;color:#1d4ed8;font-weight:800;margin-bottom:7px}}
.kpis{{display:grid;grid-template-columns:1fr 1fr;gap:6px 20px;margin-bottom:8px}}
.kpi-t{{font-size:11.5px;font-weight:700;color:#333;margin-bottom:3px;display:flex;justify-content:space-between}}
.kpi-t .win{{color:var(--jung);font-weight:800}}
.bar{{display:flex;height:25px;border-radius:5px;overflow:hidden;font-size:12px;font-weight:800;color:#fff;box-shadow:inset 0 0 0 1px rgba(0,0,0,.06)}}
.bar .bj{{background:var(--jung);display:flex;align-items:center;justify-content:center;gap:3px}}
.bar .bw{{background:var(--won);display:flex;align-items:center;justify-content:center;gap:3px}}
.kpi-sub{{font-size:11px;color:#444;margin-top:3px;text-align:right;font-weight:600}}
.trend{{display:flex;gap:8px;border-top:1px dashed #c7d6f5;padding-top:7px}}
.tcard{{flex:1;background:#fff;border:1px solid #e2e8f4;border-radius:6px;padding:5px 9px;text-align:center}}
.tcard .tl{{font-size:10px;color:var(--sub);font-weight:600;margin-bottom:1px}}
.tcard .tv{{font-size:12.5px;font-weight:800}}
.tcard .tv .ar-up{{color:var(--good)}}.tcard .tv .ar-dn{{color:var(--bad)}}.tcard .tv .old{{color:#999;font-weight:600;font-size:11.5px}}
.sw{{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:9px}}
.swbox{{border-radius:9px;padding:6px 10px}}
.swbox.good{{background:var(--goodbg);border:1.5px solid #bfe3cc}}
.swbox.bad{{background:var(--badbg);border:1.5px solid #f3c9c5}}
.swbox h3{{font-size:11.5px;font-weight:800;margin-bottom:5px;display:flex;align-items:center;gap:5px}}
.swbox.good h3{{color:var(--good)}}.swbox.bad h3{{color:var(--bad)}}
.chips{{display:flex;flex-wrap:wrap;gap:5px}}
.chip{{font-size:11px;font-weight:700;padding:3px 8px;border-radius:20px;background:#fff;border:1px solid #d6d6d6;display:flex;align-items:center;gap:4px}}
.chip b{{font-size:11.5px}}.swbox.good .chip b{{color:var(--good)}}.swbox.bad .chip b{{color:var(--bad)}}
.chip .nm{{color:#333}}.chip .it{{color:#999;font-weight:500;font-size:10px}}
section{{margin-bottom:8px}}
.stitle{{font-size:12.5px;font-weight:800;margin-bottom:4px;padding-left:9px;border-left:4px solid var(--ink)}}
.stitle small{{font-weight:600;color:var(--sub);font-size:10px;margin-left:6px}}
table{{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}}
th,td{{border:1px solid var(--line);padding:2.5px 6px;text-align:right}}
th{{background:#f1f3f6;font-weight:700;text-align:center;font-size:10px;color:#333}}
th.grp{{background:#e7edf7;color:var(--jung)}}th.grpw{{background:#f6ece4;color:var(--won)}}
td.lbl{{text-align:left;font-weight:700;font-size:11px;white-space:nowrap}}
td.sub,span.sub{{font-size:9px;color:var(--sub);font-weight:500}}
.colj{{background:#f7faff}}.colw{{background:#fdf6f0}}
td.pct{{font-weight:800;text-align:center;font-size:12px}}
td.offc{{color:#8a8a8a;font-size:11px}}
.auc tbody td{{padding-top:6.5px;padding-bottom:6.5px}}
.pct .pnm{{display:block;font-size:8.5px;font-weight:600;color:#555;margin-bottom:2px;line-height:1.12;white-space:normal;letter-spacing:-.2px}}
.p-big .pnm,.p-lose .pnm{{color:#fff;opacity:.92}}
.p-win{{color:var(--jung)}}.p-big{{color:#fff;background:var(--jung)}}.p-lose{{color:#fff;background:var(--won)}}.p-mid{{color:#555}}
tr.total td{{background:#1a1a1a;color:#fff;font-weight:800;font-size:12px;border-color:#1a1a1a}}
tr.total td.pct{{background:#1a1a1a;color:#ffd84d}}
tr.corr td{{background:#fffdf2;color:#8a6d1f;font-weight:600;font-size:10.5px}}
.cmp td{{text-align:center;font-size:12px}}.cmp td.lbl{{text-align:left}}
.cmp .du{{color:var(--good);font-weight:800}}.cmp .dd{{color:var(--bad);font-weight:800}}.cmp .vj{{color:var(--jung);font-weight:700}}
.foot{{margin-top:8px;border-top:1px solid var(--line2);padding-top:5px;display:flex;justify-content:space-between;color:var(--sub);font-size:9.5px}}
.note{{font-size:9.5px;color:var(--sub);margin-top:3px;line-height:1.45}}.note b{{color:var(--ink)}}
.srcbox{{font-size:9.5px;color:#555;margin-top:6px;background:#fbfbfd;border:1px solid var(--line2);border-radius:6px;padding:6px 9px;line-height:1.5}}
.srcbox b{{color:var(--ink)}}
@media print{{body{{background:#fff;font-size:11px}}.page{{margin:0;box-shadow:none;width:auto;min-height:auto;padding:6mm 8mm}}
@page{{size:A4;margin:0}}.bar,.p-big,.p-lose,tr.total td,.swbox.good,.swbox.bad,th.grp,th.grpw,.colj,.colw,tr.corr td{{-webkit-print-color-adjust:exact;print-color-adjust:exact}}}}
</style></head><body><div class="page">
  <div class="head">
    <div><h1>노은도매시장 거래현황 — 상반기 경매사별</h1>
      <div class="legend">중앙청과 vs 원협노은 &nbsp;·&nbsp;
        <span class="cj">■ 중앙청과㈜(우리)</span> &nbsp; <span class="cw">■ 원협노은(공)</span></div>
    </div>
    <div class="meta"><b>2026년 상반기</b> (1/1 ~ 6/30)<br>
      작성 2026-07-08 &nbsp;·&nbsp; 단위: kg · 원<br>출처: 정산자료 · aT유통공사(4·5월)</div>
  </div>
  <div class="dash">
    <div class="dash-h">📌 한눈에 보는 결론 — 2026 상반기 (1~6월 누계)</div>
    <div class="kpis">
      <div><div class="kpi-t"><span>물량 점유</span><span class="win">중앙 우세</span></div>
        <div class="bar"><span class="bj" style="width:{qty_share26:.1f}%">중앙 {qty_share26:.1f}%</span><span class="bw" style="width:{100-qty_share26:.1f}%">원협 {100-qty_share26:.1f}%</span></div></div>
      <div><div class="kpi-t"><span>금액 점유</span><span class="win">중앙 우세</span></div>
        <div class="bar"><span class="bj" style="width:{amt_share26:.1f}%">중앙 {amt_share26:.1f}%</span><span class="bw" style="width:{100-amt_share26:.1f}%">원협 {100-amt_share26:.1f}%</span></div>
        <div class="kpi-sub">중앙 <b>{eok(j26a)}억</b> vs 원협 <b>{eok(w26a)}억</b> &nbsp;( ＋{gap26:.1f}억 )</div></div>
    </div>
  </div>
  <section>
    <div class="stitle">① 경매사별 거래현황 <small>상반기 1~6월 누계 · 금액점유% = 그 품목군에서 우리 비중 (파랑=우세 / 벽돌=열세)</small></div>
    <table class="auc"><thead>
      <tr><th rowspan="2" style="width:24%">경매사 (담당 품목군)</th>
        <th class="grp" colspan="2">중앙청과㈜ (우리)</th><th class="grpw" colspan="2">원협노은(공)</th>
        <th rowspan="2" style="width:15%">경매사 · 중앙<br>금액점유</th></tr>
      <tr><th class="grp">물량(kg)</th><th class="grp">금액(원)</th><th class="grpw">물량(kg)</th><th class="grpw">금액(원)</th></tr>
    </thead><tbody>{arows}
      <tr class="corr"><td class="lbl">＋ 중앙 2/24 소실 보정 <span class="sub">(aT 원천 부재 · 회사 정산)</span></td><td class="colj">{kg(corr24['qty_kg'])}</td><td class="colj">{won(corr24['amount'])}</td><td class="colw">—</td><td class="colw">—</td><td class="pct p-mid">—</td></tr>
      <tr class="total"><td class="lbl">합　계 (보정 후)</td><td>{kg(sjq+corr24['qty_kg'])}</td><td>{won(sja+corr24['amount'])}</td><td>{kg(swq)}</td><td>{won(swa)}</td><td class="pct">{pct(sja+corr24['amount'], swa):.1f}%</td></tr>
    </tbody></table>
    <div class="note">＊ 경매사(품목군)은 우리 회사 담당 기준으로 양사 품목을 같은 군으로 묶어 비교. 경매사별 소계는 aT 원천값(추적 가능), <b>중앙 2/24 소실 5.08억(244.7톤)은 aT에 없어 경매사별 미배분</b> → 별도 보정행으로 합계에 반영. 보정 후 합계 = 회사 월계표 공식(835.7억/38,241톤)과 일치.</div>
  </section>
  <section>
    <div class="stitle">② 월별 흐름 <small>2026 1~6월, 중앙 vs 원협 금액(억) · 재집계 / 관리사업소 공식 병기</small></div>
    <table><thead>
      <tr><th rowspan="2" style="width:18%">월</th><th class="grp" colspan="2">중앙청과 (억)</th><th class="grpw" colspan="2">원협노은 (억)</th><th rowspan="2" style="width:13%">중앙 점유</th></tr>
      <tr><th class="grp">재집계</th><th class="grp">관리사업소</th><th class="grpw">재집계</th><th class="grpw">관리사업소</th></tr>
    </thead><tbody>{mrows}
      <tr class="total"><td class="lbl">상반기 누계</td><td>{eok(j26a)}</td><td>—</td><td>{eok(w26a)}</td><td>—</td><td class="pct">{amt_share26:.1f}%</td></tr>
    </tbody></table>
    <div class="note">→ 정정 후 <b>6개월 모두 중앙청과 우세</b>. <b>재집계</b>=우리 수집(원협 4·5월 aT유통공사) · <b>관리사업소</b>=노은시장 관리사업소 공식 현황판(1~5월 제공, 6월 미제공). <b style="color:#b45309">원협 4·5월</b>은 재집계(온라인 제외)가 관리사업소 공식보다 소폭 낮음(온라인 전자거래 차이, 약 3~4억/월).</div>
  </section>
  <div class="srcbox">
    <b>📌 데이터 출처·정정 안내</b> &nbsp;(값이 자료마다 다소 다를 수 있어 정정 기준을 명시)<br>
    · <b>원협 4·5월</b> = aT유통공사 '도매시장 거래현황' 자료 (경매+정가수의). 우리 data.go.kr 수집은 계통출하 이중집계로 과대(4월 189.9억)여서 <b>122.5·125.7억으로 교체</b>. 단 aT유통공사엔 온라인 전자거래가 빠져 노은 관리사업소 공식(4월 126.2억)보다 소폭 낮음(온라인 누락 추정, 큰 영향 아님).<br>
    · <b>중앙 2/24</b> = aT 원천에 없어(소실) 회사 정산 자료로 5.08억/244.7톤 보정.<br>
    · 나머지(중앙 전월·원협 1·2·3·6월) = data.go.kr 수집 정산자료. <b>보정 후 상반기 총계 = 회사 월계표 공식과 일치.</b>
  </div>
  <div class="foot"><div>대전중앙청과 · 노은도매시장 거래현황 (2026 상반기, 정산자료 기준)</div>
    <div>제작 터미널: WHOLESALE-T3 · 2026-07-08</div></div>
</div></body></html>"""
    return HTML


if __name__ == "__main__":
    html = build()
    out_dir = os.path.join("C:/Users/samsung/2026/02/monet", "presentations",
                           "noeun-halfyear-2026-central-vs-wonhyup-2026-07-08")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, "index.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    dl = os.path.join(DL, "노은도매시장_거래현황_2026상반기_중앙vs원협_2026-07-08.html")
    with open(dl, "w", encoding="utf-8") as f:
        f.write(html)
    print("saved:", out)
    print("saved:", dl)
