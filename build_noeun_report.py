# -*- coding: utf-8 -*-
"""노은도매시장 거래현황 한 장 보고서 자동 생성 (중앙청과 vs 원협노은, 경매사별 + 작년 대비).

settlement_report.py의 AUCTION_BLOCKS/load_range/auction_block_index를 그대로 재사용 = 정산메일과
동일 로직 = 회장님/사장님 보고용 수치 정합 보장 (6/26 수동본과 완전 일치 검증 통과).

사용법:
  python build_noeun_report.py                   # 자동(6월 누계, D+3 안정 마지막 정산일까지)
  python build_noeun_report.py --end 2026-06-23  # 종료일 강제 (6/26 수동본 재현용)
  python build_noeun_report.py --verify          # 6/1~6/23 집계 vs 6/26 수동본 수치 대조(집계만)
"""
import sys, io, os, re, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import settlement_report as sr
from datetime import date
from collections import defaultdict

J, W = "25000301", "25000302"   # 중앙청과(우리), 원협노은
MONET = "C:/Users/samsung/2026/02/monet"


def clean_label(label):
    return re.sub(r'^\d\d:\d\d\s*', '', label).strip()


def agg_auctioneer(records):
    """경매사 라벨별 중앙/원협 [qty_kg, amount] + 취급 품목금액 + 표시순서."""
    data = defaultdict(lambda: {J: [0.0, 0.0], W: [0.0, 0.0]})
    prods = defaultdict(lambda: defaultdict(float))
    order = {}
    for r in records:
        code = r.get('corp_code')
        if code not in (J, W):
            continue
        product = r.get('product', '기타') or '기타'
        cc = (r.get('category_code') or '').strip()
        bidx = sr.auction_block_index(product, cc)
        label = sr.AUCTION_BLOCKS[bidx][3] if bidx < len(sr.AUCTION_BLOCKS) else "미배정"
        if label not in order:
            order[label] = sr.auction_label_order(product, cc)
        q = r.get('total_qty', 0) or 0
        a = r.get('total_amount', 0) or 0
        data[label][code][0] += q
        data[label][code][1] += a
        prods[label][product] += a
    return data, order, prods


def totals(data):
    jq = sum(v[J][0] for v in data.values()); ja = sum(v[J][1] for v in data.values())
    wq = sum(v[W][0] for v in data.values()); wa = sum(v[W][1] for v in data.values())
    return jq, ja, wq, wa


def snapshot_totals(start, end):
    """아카이브에 없는 과거 기간(GitHub Actions data/는 최근 몇 달만)의 노은 2법인 합계를
    data/noeun_prev_snapshot.json(일별, make_noeun_snapshot.py로 로컬 생성)에서 계산.
    스냅샷 범위 밖이면 None."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'noeun_prev_snapshot.json')
    if not os.path.exists(path):
        return None
    import json
    with open(path, encoding='utf-8') as f:
        snap = json.load(f)
    s, e = start.isoformat(), end.isoformat()
    jq = ja = wq = wa = 0.0
    hit = False
    for d, corps in snap.items():
        if s <= d <= e:
            hit = True
            q, a = corps.get(J, [0, 0]); jq += q; ja += a
            q, a = corps.get(W, [0, 0]); wq += q; wa += a
    return (jq, ja, wq, wa) if hit else None


def load_auct_snapshot(start, end):
    """GitHub Actions(data/에 작년 파일 없음)에서 작년 경매사별 값을 대체할 스냅샷 로드.
    make_noeun_snapshot.py --auctioneer 로 로컬 아카이브에서 미리 생성해 커밋해 둔 파일.
    범위 밖이면 None."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'noeun_prev_auct_snapshot.json')
    if not os.path.exists(path):
        return None
    import json
    with open(path, encoding='utf-8') as f:
        snap = json.load(f)
    s, e = start.isoformat(), end.isoformat()
    corp = defaultdict(lambda: {J: [0.0, 0.0], W: [0.0, 0.0]})
    hit = False
    for d, labels in snap.items():
        if s <= d <= e:
            hit = True
            for lb, v in labels.items():
                corp[lb][J][0] += v['J'][0]; corp[lb][J][1] += v['J'][1]
                corp[lb][W][0] += v['W'][0]; corp[lb][W][1] += v['W'][1]
    return dict(corp) if hit else None


def pcls(p):
    return 'p-big' if p >= 55 else 'p-win' if p >= 50 else 'p-mid' if p >= 47 else 'p-lose'


def prod_group(pd, n=3):
    top = sorted(pd.items(), key=lambda x: -x[1])[:n]
    return '·'.join(p for p, _ in top)


def f0(n):
    return f'{n:,.0f}'


CSS = """
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
:root{--ink:#1a1a1a;--sub:#666;--line:#cfcfcf;--line2:#e8e8e8;
  --jung:#1d4ed8;--jung2:#3b6fe0;--won:#c2622f;--won2:#d98a52;
  --hi:#fff7cc;--good:#15803d;--goodbg:#e9f7ee;--bad:#b91c1c;--badbg:#fdeceb;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Pretendard',sans-serif;color:var(--ink);background:#e9ecf1;font-size:11.5px;line-height:1.32;-webkit-font-smoothing:antialiased}
.page{width:210mm;min-height:297mm;margin:14px auto;background:#fff;padding:8mm 10mm 6mm;box-shadow:0 2px 16px rgba(0,0,0,.18)}
h1{font-size:18px;font-weight:800;letter-spacing:-.5px}
.head{display:flex;justify-content:space-between;align-items:flex-end;border-bottom:2.5px solid var(--ink);padding-bottom:5px;margin-bottom:8px}
.head .meta{text-align:right;color:var(--sub);font-size:11px;line-height:1.5}
.head .meta b{color:var(--ink)}
.legend{font-size:10.5px;color:var(--sub);margin-top:3px}
.legend .cj{color:var(--jung);font-weight:700}.legend .cw{color:var(--won);font-weight:700}
.dash{border:1.5px solid #c7d6f5;border-radius:9px;padding:8px 12px 9px;margin-bottom:9px;background:linear-gradient(180deg,#f7faff,#fff)}
.dash-h{font-size:12.5px;color:#1d4ed8;font-weight:800;margin-bottom:7px}
.kpis{display:grid;grid-template-columns:1fr 1fr;gap:6px 20px;margin-bottom:8px}
.kpi-t{font-size:11.5px;font-weight:700;color:#333;margin-bottom:3px;display:flex;justify-content:space-between}
.kpi-t .win{color:var(--jung);font-weight:800}
.bar{display:flex;height:25px;border-radius:5px;overflow:hidden;font-size:12px;font-weight:800;color:#fff;box-shadow:inset 0 0 0 1px rgba(0,0,0,.06)}
.bar .bj{background:var(--jung);display:flex;align-items:center;justify-content:center;gap:3px}
.bar .bw{background:var(--won);display:flex;align-items:center;justify-content:center;gap:3px}
.kpi-sub{font-size:11px;color:#444;margin-top:3px;text-align:right;font-weight:600}
.trend{display:flex;gap:8px;border-top:1px dashed #c7d6f5;padding-top:7px}
.tcard{flex:1;background:#fff;border:1px solid #e2e8f4;border-radius:6px;padding:5px 9px;text-align:center}
.tcard .tl{font-size:10px;color:var(--sub);font-weight:600;margin-bottom:1px}
.tcard .tv{font-size:12.5px;font-weight:800}
.tcard .tv .ar-up{color:var(--good)}.tcard .tv .ar-dn{color:var(--bad)}.tcard .tv .old{color:#999;font-weight:600;font-size:11.5px}
.sw{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:9px}
.swbox{border-radius:9px;padding:6px 10px}
.swbox.good{background:var(--goodbg);border:1.5px solid #bfe3cc}
.swbox.bad{background:var(--badbg);border:1.5px solid #f3c9c5}
.swbox h3{font-size:11.5px;font-weight:800;margin-bottom:5px;display:flex;align-items:center;gap:5px}
.swbox.good h3{color:var(--good)}.swbox.bad h3{color:var(--bad)}
.chips{display:flex;flex-wrap:wrap;gap:5px}
.chip{font-size:11px;font-weight:700;padding:3px 8px;border-radius:20px;background:#fff;border:1px solid #d6d6d6;display:flex;align-items:center;gap:4px}
.chip b{font-size:11.5px}.swbox.good .chip b{color:var(--good)}.swbox.bad .chip b{color:var(--bad)}
.chip .nm{color:#333}.chip .it{color:#999;font-weight:500;font-size:10px}
section{margin-bottom:8px}
.stitle{font-size:12.5px;font-weight:800;margin-bottom:4px;padding-left:9px;border-left:4px solid var(--ink)}
.stitle small{font-weight:600;color:var(--sub);font-size:10px;margin-left:6px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
th,td{border:1px solid var(--line);padding:2.5px 6px;text-align:right}
th{background:#f1f3f6;font-weight:700;text-align:center;font-size:10px;color:#333}
th.grp{background:#e7edf7;color:var(--jung)}th.grpw{background:#f6ece4;color:var(--won)}
td.lbl{text-align:left;font-weight:700;font-size:11px;white-space:nowrap}
td.sub,span.sub{font-size:9px;color:var(--sub);font-weight:500}
.colj{background:#f7faff}.colw{background:#fdf6f0}
td.pct{font-weight:800;text-align:center;font-size:12px}
.p-win{color:var(--jung)}.p-big{color:#fff;background:var(--jung)}.p-lose{color:#fff;background:var(--won)}.p-mid{color:#555}
tr.total td{background:#1a1a1a;color:#fff;font-weight:800;font-size:12px;border-color:#1a1a1a}
tr.total td.pct{background:#1a1a1a;color:#ffd84d}
tr.rest td{background:#fafafa;color:#999}
.cmp td{text-align:center;font-size:12px}.cmp td.lbl{text-align:left}
.cmp .du{color:var(--good);font-weight:800}.cmp .dd{color:var(--bad);font-weight:800}.cmp .vj{color:var(--jung);font-weight:700}
.foot{margin-top:8px;border-top:1px solid var(--line2);padding-top:5px;display:flex;justify-content:space-between;color:var(--sub);font-size:9.5px}
.note{font-size:9.5px;color:var(--sub);margin-top:3px;line-height:1.4}.note b{color:var(--ink)}
@media print{body{background:#fff;font-size:11px}.page{margin:0;box-shadow:none;width:auto;min-height:auto;padding:6mm 8mm}
@page{size:A4;margin:0}.bar,.p-big,.p-lose,tr.total td,.swbox.good,.swbox.bad,th.grp,th.grpw,.colj,.colw{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
"""


def generate_html(end_date):
    start = date(end_date.year, end_date.month, 1)   # 해당 월 1일 (6월 하드코딩 제거 — 월 경계)
    recs, days = sr.load_range(start, end_date)
    data, order, prods = agg_auctioneer(recs)
    # 작년 동기 (같은 월/일)
    start25 = date(end_date.year - 1, end_date.month, 1)
    end25 = date(end_date.year - 1, end_date.month, end_date.day)
    recs25, days25 = sr.load_range(start25, end25)
    data25, _, _ = agg_auctioneer(recs25)

    jq, ja, wq, wa = totals(data)
    jq25, ja25, wq25, wa25 = totals(data25)
    if not recs25:
        # GitHub Actions: data/에 작년 파일이 없어 빈 집계(0%) → 스냅샷으로 대체
        snap = snapshot_totals(start25, end25)
        if snap:
            jq25, ja25, wq25, wa25 = snap
    vol = jq / (jq + wq) * 100; amt = ja / (ja + wa) * 100
    vol25 = jq25 / (jq25 + wq25) * 100 if (jq25 + wq25) else 0
    amt25 = ja25 / (ja25 + wa25) * 100 if (ja25 + wa25) else 0
    gap = (ja - wa) / 1e8; gap25 = (ja25 - wa25) / 1e8

    # 경매사별 행
    labels = sorted(order, key=lambda x: order[x])
    rows = ''; sw = []
    for lb in labels:
        d = data[lb]; js, ws = d[J], d[W]
        denom = js[1] + ws[1]
        sh = js[1] / denom * 100 if denom else 0
        clean = clean_label(lb); grp = prod_group(prods[lb])
        is_rest = ('나머지' in lb) or ('미배정' in lb)
        if is_rest:
            rows += (f'<tr class="rest"><td class="lbl">{clean} <span class="sub">({grp})</span></td>'
                     f'<td>{f0(js[0])}</td><td>{f0(js[1])}</td><td>{f0(ws[0])}</td><td>{f0(ws[1])}</td>'
                     f'<td class="pct">{sh:.1f}%</td></tr>')
        else:
            rows += (f'<tr><td class="lbl">{clean} <span class="sub">({grp})</span></td>'
                     f'<td class="colj">{f0(js[0])}</td><td class="colj">{f0(js[1])}</td>'
                     f'<td class="colw">{f0(ws[0])}</td><td class="colw">{f0(ws[1])}</td>'
                     f'<td class="pct {pcls(sh)}">{sh:.1f}%</td></tr>')
            sw.append((clean, grp, sh))
    rows += (f'<tr class="total"><td class="lbl">합　계</td><td>{f0(jq)}</td><td>{f0(ja)}</td>'
             f'<td>{f0(wq)}</td><td>{f0(wa)}</td><td class="pct">{amt:.1f}%</td></tr>')

    sw_sorted = sorted(sw, key=lambda x: -x[2])
    strong = sw_sorted[:5]; weak = sw_sorted[-5:][::-1]
    def chips(items):
        return ''.join(f'<span class="chip"><span class="nm">{c}</span>'
                       f'<span class="it">{g}</span><b>{s:.0f}%</b></span>' for c, g, s in items)

    def arrow(now, old, good_up=True):
        up = now >= old
        cls = 'ar-up' if (up == good_up) else 'ar-dn'
        return cls, ('▲' if up else '▽')
    a_amt = arrow(amt, amt25); a_vol = arrow(vol, vol25); a_gap = arrow(gap, gap25)
    amt_txt = '역전' if (amt >= 50 and amt25 < 50) else ('우세' if amt >= 50 else '열세')
    vol_txt = '우세' if vol >= 50 else '열세'

    today = date.today()
    period = f"{start.month}/{start.day} ~ {end_date.month}/{end_date.day}"

    html = f"""<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>노은도매시장 거래현황 — 중앙청과 vs 원협노은 (경매사별 {end_date.month}월 누계)</title>
<style>{CSS}</style></head><body><div class="page">
  <div class="head">
    <div><h1>노은도매시장 거래현황 — 경매사별</h1>
      <div class="legend">중앙청과 vs 원협노은 &nbsp;·&nbsp;
        <span class="cj">■ 중앙청과㈜(우리)</span> &nbsp; <span class="cw">■ 원협노은(공)</span></div>
    </div>
    <div class="meta"><b>{end_date.year}년 {end_date.month}월 누계</b> ({period}, {days}영업일)<br>
      작성 {today.month}/{today.day} &nbsp;·&nbsp; 단위: kg · 원<br>출처: 도매시장통합 정산자료</div>
  </div>
  <div class="dash">
    <div class="dash-h">📌 한눈에 보는 결론 — {end_date.month}월 누계 ({end_date.month}/{end_date.day}까지)</div>
    <div class="kpis">
      <div><div class="kpi-t"><span>물량 점유</span><span class="win">중앙 {vol_txt} {'▲' if vol>=50 else '▽'}</span></div>
        <div class="bar"><span class="bj" style="width:{vol:.1f}%">중앙 {vol:.1f}%</span><span class="bw" style="width:{100-vol:.1f}%">원협 {100-vol:.1f}%</span></div></div>
      <div><div class="kpi-t"><span>금액 점유</span><span class="win">중앙 {amt_txt} {'▲' if amt>=50 else '▽'}</span></div>
        <div class="bar"><span class="bj" style="width:{amt:.1f}%">중앙 {amt:.1f}%</span><span class="bw" style="width:{100-amt:.1f}%">원협 {100-amt:.1f}%</span></div>
        <div class="kpi-sub">중앙 <b>{ja/1e8:.1f}억</b> vs 원협 <b>{wa/1e8:.1f}억</b> &nbsp;( {'＋' if gap>=0 else '−'}{abs(gap):.1f}억 )</div></div>
    </div>
    <div class="trend">
      <div class="tcard"><div class="tl">작년 대비 · 금액 점유</div>
        <div class="tv"><span class="old">{amt25:.1f}%</span> → {amt:.1f}% <span class="{a_amt[0]}">{a_amt[1]} {amt_txt}</span></div></div>
      <div class="tcard"><div class="tl">작년 대비 · 물량 점유</div>
        <div class="tv"><span class="old">{vol25:.1f}%</span> → {vol:.1f}% <span class="{a_vol[0]}">{a_vol[1]}</span></div></div>
      <div class="tcard"><div class="tl">작년 대비 · 금액 격차</div>
        <div class="tv"><span class="old">{gap25:+.1f}억</span> → {gap:+.1f}억 <span class="{a_gap[0]}">{a_gap[1]}</span></div></div>
    </div>
  </div>
  <div class="sw">
    <div class="swbox good"><h3>💪 우리가 강한 품목군 <small style="font-weight:600;color:#888;font-size:10px">(금액점유 높은 순)</small></h3>
      <div class="chips">{chips(strong)}</div></div>
    <div class="swbox bad"><h3>⚠️ 우리가 약한 품목군 <small style="font-weight:600;color:#888;font-size:10px">(금액점유 낮은 순)</small></h3>
      <div class="chips">{chips(weak)}</div></div>
  </div>
  <section>
    <div class="stitle">① 경매사별 거래현황 <small>{end_date.month}월 누계 · 금액점유% = 그 품목군에서 우리 비중 (파랑=우세 / 벽돌=열세)</small></div>
    <table><thead>
      <tr><th rowspan="2" style="width:25%">경매사 (담당 품목군)</th>
        <th class="grp" colspan="2">중앙청과㈜ (우리)</th><th class="grpw" colspan="2">원협노은(공)</th>
        <th rowspan="2" style="width:10%">중앙<br>금액점유</th></tr>
      <tr><th class="grp">물량(kg)</th><th class="grp">금액(원)</th><th class="grpw">물량(kg)</th><th class="grpw">금액(원)</th></tr>
    </thead><tbody>{rows}</tbody></table>
    <div class="note">＊ 경매사(품목군)은 우리 회사 담당 기준으로 양사 품목을 같은 군으로 묶어 비교한 것. 품목군은 그 경매사 취급 품목 중 금액 상위. 소계 합 = 전체 총액·총물량과 일치.</div>
  </section>
  <section>
    <div class="stitle">② 작년 동기 대비 흐름 <small>{end_date.year-1} vs {end_date.year}, {period} 누계 (중앙 : 원협)</small></div>
    <table class="cmp"><thead><tr><th style="width:28%">구분</th><th>{end_date.year-1}년 (작년)</th><th>{end_date.year}년 (올해)</th><th style="width:22%">변화</th></tr></thead><tbody>
      <tr><td class="lbl">물량 점유율 (중앙 : 원협)</td><td>{vol25:.1f} : {100-vol25:.1f}</td><td class="vj">{vol:.1f} : {100-vol:.1f}</td><td class="{'du' if vol>=vol25 else 'dd'}">{a_vol[1]}</td></tr>
      <tr><td class="lbl">금액 점유율 (중앙 : 원협)</td><td>{amt25:.1f} : {100-amt25:.1f}</td><td class="vj">{amt:.1f} : {100-amt:.1f}</td><td class="{'du' if amt>=amt25 else 'dd'}">{a_amt[1]} {amt_txt}</td></tr>
      <tr><td class="lbl">금액 격차 (중앙 − 원협)</td><td>{gap25:+.1f}억</td><td>{gap:+.1f}억</td><td class="{'du' if gap>=gap25 else 'dd'}">{gap-gap25:+.1f}억 {a_gap[1]}</td></tr>
      <tr><td class="lbl">중앙청과 금액 (누계)</td><td>{ja25/1e8:.1f}억</td><td>{ja/1e8:.1f}억</td><td class="{'du' if ja>=ja25 else 'dd'}">{(ja-ja25)/1e8:+.1f}억</td></tr>
      <tr><td class="lbl">원협노은 금액 (누계)</td><td>{wa25/1e8:.1f}억</td><td>{wa/1e8:.1f}억</td><td>{(wa-wa25)/1e8:+.1f}억</td></tr>
    </tbody></table>
    <div class="note">→ 금액 점유율 {amt25:.1f}% → {amt:.1f}% ({amt_txt}). 작년 동기 대비 중앙청과 금액 {(ja-ja25)/1e8:+.1f}억.</div>
  </section>
  <div class="foot"><div>대전중앙청과 · 노은도매시장 거래현황 (정산자료 기준, 자동 생성)</div>
    <div>금액점유: <span style="color:var(--jung);font-weight:700">파랑=우세</span> / <span style="color:#555">회색=박빙</span> / <span style="color:var(--won);font-weight:700">벽돌=열세</span></div></div>
</div></body></html>"""
    return html, dict(start=start, end=end_date, days=days, vol=vol, amt=amt,
                      ja=ja, wa=wa, amt25=amt25, vol25=vol25)


def agg_auctioneer_detail(records):
    """경매사 라벨별: 중앙/원협 합계(corp) + 품목별 중앙/원협 [qty,amt](prod) + 표시순서."""
    corp = defaultdict(lambda: {J: [0.0, 0.0], W: [0.0, 0.0]})
    prod = defaultdict(lambda: defaultdict(lambda: {J: [0.0, 0.0], W: [0.0, 0.0]}))
    order = {}
    for r in records:
        code = r.get('corp_code')
        if code not in (J, W):
            continue
        product = r.get('product', '기타') or '기타'
        cc = (r.get('category_code') or '').strip()
        bidx = sr.auction_block_index(product, cc)
        label = sr.AUCTION_BLOCKS[bidx][3] if bidx < len(sr.AUCTION_BLOCKS) else "미배정"
        if label not in order:
            order[label] = sr.auction_label_order(product, cc)
        q = r.get('total_qty', 0) or 0
        a = r.get('total_amount', 0) or 0
        corp[label][code][0] += q; corp[label][code][1] += a
        prod[label][product][code][0] += q; prod[label][product][code][1] += a
    return corp, prod, order


def losing_products(items):
    """품목dict → 원협에 금액으로 진 품목(품목 많으면 물량 상위 15개 범위 내), 많이 지는 순."""
    ranked = sorted(items.items(), key=lambda x: -(x[1][J][0] + x[1][W][0]))
    scope = ranked[:15] if len(ranked) > 15 else ranked
    losing = [(p, d) for p, d in scope if d[J][1] < d[W][1]]
    losing.sort(key=lambda x: x[1][J][1] - x[1][W][1])
    return losing


def generate_manager_html(end):
    """관리자용 — 원협에 지는 품목 원인 분석 (강/약배지·작년대비 제거, 진품목·당일물량 추가)."""
    start = date(end.year, end.month, 1)   # 해당 월 1일 (6월 하드코딩 제거 — 월 경계)
    recs, days = sr.load_range(start, end)
    corp, prod, order = agg_auctioneer_detail(recs)
    recs_d, _ = sr.load_range(end, end)
    corp_d, _, _ = agg_auctioneer_detail(recs_d)
    # 작년 동기(같은 월/일) 경매사별 — 로컬 아카이브에서 직접. GA(data/에 작년 없음)면 빈 dict → 병기 생략.
    start25 = date(end.year - 1, end.month, 1)
    end25 = date(end.year - 1, end.month, end.day)
    recs25, _ = sr.load_range(start25, end25)
    corp25, _, _ = agg_auctioneer_detail(recs25)
    has_prev = bool(recs25)
    if not has_prev:
        # GitHub Actions: data/에 작년 파일 없음 → 경매사별 스냅샷으로 대체
        snap25 = load_auct_snapshot(start25, end25)
        if snap25:
            corp25 = snap25
            has_prev = True

    def cell(now, old, div, dec):
        """올해 값(그대로 크기) + 작년 값(회색 작게 병기). 단위 축소로 자릿수↓ = 줄바꿈 없이 한 칸."""
        cur = f'{now/div:,.{dec}f}'
        if has_prev:
            return f'{cur}<span class="yoy">/{old/div:,.{dec}f}</span>'
        return cur

    jq = sum(v[J][0] for v in corp.values()); ja = sum(v[J][1] for v in corp.values())
    wq = sum(v[W][0] for v in corp.values()); wa = sum(v[W][1] for v in corp.values())
    vol = jq / (jq + wq) * 100 if (jq + wq) else 0
    amt = ja / (ja + wa) * 100 if (ja + wa) else 0
    djq = sum(v[J][0] for v in corp_d.values()); dwq = sum(v[W][0] for v in corp_d.values())
    dja = sum(v[J][1] for v in corp_d.values()); dwa = sum(v[W][1] for v in corp_d.values())
    dvol = djq / (djq + dwq) * 100 if (djq + dwq) else 0
    damt = dja / (dja + dwa) * 100 if (dja + dwa) else 0

    labels = sorted(order, key=lambda x: order[x])
    rows = ''
    for lb in labels:
        c = corp[lb]; js, ws = c[J], c[W]
        c25 = corp25.get(lb, {J: [0.0, 0.0], W: [0.0, 0.0]})
        js25, ws25 = c25[J], c25[W]
        denom = js[1] + ws[1]
        sh = js[1] / denom * 100 if denom else 0
        clean = clean_label(lb)
        rows += (f'<tr><td class="lbl">{clean}</td>'
                 f'<td class="colj">{cell(js[0], js25[0], 1000, 1)}</td>'
                 f'<td class="colj">{cell(js[1], js25[1], 1e6, 0)}</td>'
                 f'<td class="colw">{cell(ws[0], ws25[0], 1000, 1)}</td>'
                 f'<td class="colw">{cell(ws[1], ws25[1], 1e6, 0)}</td>'
                 f'<td class="pct {pcls(sh)}">{sh:.1f}%</td></tr>')
        losing = losing_products(prod[lb])
        if losing:
            def _ratio(d):
                t = d[J][1] + d[W][1]
                jj = round(d[J][1] / t * 100) if t else 0
                return f'{jj}:{100 - jj}'
            chips = ' &nbsp;/&nbsp; '.join(f'{p} <b>{_ratio(d)}</b>' for p, d in losing)
            rows += f'<tr class="losing"><td colspan="6">{chips}</td></tr>'

    today = date.today()
    html = f"""<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>노은도매시장 경매사별 열세 품목 분석 (관리자용)</title>
<style>{CSS}
tr.losing td{{background:#f6f6f6;color:#222;font-size:11px;text-align:left;padding:6px 10px;line-height:1.75}}
tr.losing b{{color:#0d47a1;font-weight:700;font-size:11.5px}}
tr.losing .lh{{color:#666;font-weight:700}}
.yoy{{color:inherit;font-weight:inherit;font-size:inherit;margin-left:2px}}
th .uh{{font-weight:600;color:#8a8a8a;font-size:8.5px;display:block;margin-top:1px}}</style></head><body><div class="page">
  <div class="head">
    <div><h1>노은도매시장 경매사별 열세 품목 분석</h1>
      <div class="legend">관리자용 · 우리가 <b style="color:#0d47a1">지는 품목</b> 원인 분석 &nbsp;·&nbsp;
        <span class="cj">■ 중앙청과(우리)</span> <span class="cw">■ 원협</span></div></div>
    <div class="meta"><b>{end.year}년 {end.month}월 누계</b> ({start.month}/{start.day} ~ {end.month}/{end.day}, {days}영업일)<br>
      작성 {today.month}/{today.day} · 단위: kg · 원<br>출처: 도매시장통합 정산자료</div>
  </div>
  <div class="dash">
    <div class="dash-h">📌 한눈에 보는 결론 (중앙청과 : 원협)</div>
    <div class="kpis">
      <div><div class="kpi-t"><span>물량 · 이달 누계</span><span class="win">중앙 {'▲' if vol>=50 else '▽'} {vol:.1f}%</span></div>
        <div class="bar"><span class="bj" style="width:{vol:.1f}%">중앙 {vol:.1f}%</span><span class="bw" style="width:{100-vol:.1f}%">원협 {100-vol:.1f}%</span></div></div>
      <div><div class="kpi-t"><span>물량 · {end.month}/{end.day} 당일</span><span class="win">중앙 {'▲' if dvol>=50 else '▽'} {dvol:.1f}%</span></div>
        <div class="bar"><span class="bj" style="width:{dvol:.1f}%">중앙 {dvol:.1f}%</span><span class="bw" style="width:{100-dvol:.1f}%">원협 {100-dvol:.1f}%</span></div></div>
      <div><div class="kpi-t"><span>금액 · 이달 누계</span><span class="win">중앙 {'▲' if amt>=50 else '▽'} {amt:.1f}%</span></div>
        <div class="bar"><span class="bj" style="width:{amt:.1f}%">중앙 {amt:.1f}%</span><span class="bw" style="width:{100-amt:.1f}%">원협 {100-amt:.1f}%</span></div></div>
      <div><div class="kpi-t"><span>금액 · {end.month}/{end.day} 당일</span><span class="win">중앙 {'▲' if damt>=50 else '▽'} {damt:.1f}%</span></div>
        <div class="bar"><span class="bj" style="width:{damt:.1f}%">중앙 {damt:.1f}%</span><span class="bw" style="width:{100-damt:.1f}%">원협 {100-damt:.1f}%</span></div></div>
    </div>
  </div>
  <section>
    <div class="stitle">① 경매사별 거래현황 + 진 품목 (중앙청과 : 원협) <small>금액점유% = 경매사별 우리 비중 / 아래 줄 = 우리가 진 품목 (금액 점유 비율, 합 100){' · 물량·금액 = 올해/작년 동기 병기' if has_prev else ''}</small></div>
    <table><thead>
      <tr><th style="width:20%">경매사 (담당)</th>
        <th class="grp">중앙 물량<span class="uh">톤{' · 올해/작년' if has_prev else ''}</span></th><th class="grp">중앙 금액<span class="uh">백만원{' · 올해/작년' if has_prev else ''}</span></th>
        <th class="grpw">원협 물량<span class="uh">톤{' · 올해/작년' if has_prev else ''}</span></th><th class="grpw">원협 금액<span class="uh">백만원{' · 올해/작년' if has_prev else ''}</span></th>
        <th style="width:9%">중앙<br>금액점유</th></tr>
    </thead><tbody>{rows}</tbody></table>
    <div class="note">＊ 물량=톤, 금액=백만원. {'슬래시(/) 뒤 = 작년 동기(같은 기간 1~' + str(end.day) + '일). ' if has_prev else ''}서병수·김선우 부장 등 품목 많은 경매사는 물량 상위 15개 중에서만 '진 품목' 표시(엽채류 전량 제외). 과일 파트는 대체로 우세.</div>
  </section>
  <section>
    <div class="stitle">② 물량·금액 점유 — 당일 + 이달 누계 <small>(작년 대비 대신)</small></div>
    <table class="cmp"><thead><tr><th style="width:30%">구분</th><th>중앙청과</th><th>원협</th></tr></thead><tbody>
      <tr><td class="lbl">{end.month}/{end.day} 당일 물량</td><td class="vj">{djq/1000:.1f}톤 ({dvol:.1f}%)</td><td>{dwq/1000:.1f}톤 ({100-dvol:.1f}%)</td></tr>
      <tr><td class="lbl">{end.month}/{end.day} 당일 금액</td><td class="vj">{dja/1e8:.1f}억 ({damt:.1f}%)</td><td>{dwa/1e8:.1f}억 ({100-damt:.1f}%)</td></tr>
      <tr><td class="lbl">{end.month}월 누계 물량</td><td class="vj">{jq/1000:.1f}톤 ({vol:.1f}%)</td><td>{wq/1000:.1f}톤 ({100-vol:.1f}%)</td></tr>
      <tr><td class="lbl">{end.month}월 누계 금액</td><td class="vj">{ja/1e8:.1f}억 ({amt:.1f}%)</td><td>{wa/1e8:.1f}억 ({100-amt:.1f}%)</td></tr>
    </tbody></table>
  </section>
  <div class="foot"><div>대전중앙청과 · 노은도매시장 경매사별 열세 품목 분석 (관리자용, 자동 생성)</div>
    <div>회색 줄 = 우리가 진 품목 (중앙 : 원협 금액 점유 비율, 합 100) — 원인 분석 대상</div></div>
</div></body></html>"""
    return html, dict(start=start, end=end, days=days, vol=vol, dvol=dvol,
                      jq=jq, wq=wq, djq=djq, dwq=dwq)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--end')
    ap.add_argument('--verify', action='store_true')
    ap.add_argument('--manager', action='store_true')
    args = ap.parse_args()

    if args.verify:
        recs, days = sr.load_range(date(2026, 6, 1), date(2026, 6, 23))
        data, order, _ = agg_auctioneer(recs)
        jq, ja, wq, wa = totals(data)
        ok = (abs(jq-5658844) < 1 and abs(ja-9691049766) < 1 and abs(wq-5256562) < 1 and abs(wa-9387678656) < 1)
        print(f"[검증] 6/1~6/23 자동집계: 중앙 {jq:,.0f}kg/{ja:,.0f}원 · 원협 {wq:,.0f}kg/{wa:,.0f}원 "
              f"· 금액 {ja/(ja+wa)*100:.1f}% ===> {'✅ 6/26 수동본과 완전 일치' if ok else '⚠️ 불일치'}")
        sys.exit(0)

    end = date.fromisoformat(args.end) if args.end else sr.resolve_report_range()[1]

    if args.manager:
        html, meta = generate_manager_html(end)
        outdir = os.path.join(MONET, 'presentations', f'noeun-manager-report-{end.isoformat()}')
        os.makedirs(outdir, exist_ok=True)
        outpath = os.path.join(outdir, 'index.html')
        with open(outpath, 'w', encoding='utf-8') as f:
            f.write(html)
        dl = os.path.join('C:/Users/samsung/Downloads', f'노은도매시장_관리자용_열세품목_{end.isoformat()}.html')
        with open(dl, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"✅ 관리자용(열세품목) 보고서: {end.year}년 {end.month}월 누계 · 누계 물량 {meta['vol']:.1f}% / 당일 {meta['dvol']:.1f}%")
        print(f"   {outpath}")
        print(f"   {dl}")
        sys.exit(0)

    html, meta = generate_html(end)
    outdir = os.path.join(MONET, 'presentations', f'noeun-market-report-{end.isoformat()}')
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, 'index.html')
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(html)
    # Downloads 사본 (회의/인쇄용)
    dl = os.path.join('C:/Users/samsung/Downloads',
                      f'노은도매시장_거래현황_{end.year}년{end.month}월누계_{end.isoformat()}.html')
    with open(dl, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ 노은 보고서 생성: {end.year}년 {end.month}월 누계 ({meta['start']}~{meta['end']}, {meta['days']}영업일)")
    print(f"   금액점유 중앙 {meta['amt']:.1f}% (작년 {meta['amt25']:.1f}%) · 물량 {meta['vol']:.1f}% · 중앙 {meta['ja']/1e8:.1f}억 vs 원협 {meta['wa']/1e8:.1f}억")
    print(f"   {outpath}")
    print(f"   {dl}")
