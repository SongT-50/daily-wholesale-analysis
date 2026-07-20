# 수입과일 판매 실적 HTML v2 — 대전중앙청과(안대명·심세영) + 원협노은 병렬, 2026 상반기.
# 품목 = 수입과일15 + 다래 + 수입땅콩 + 수입곶감 + 수입호두(태은이 2026-07-20 추가).
# 집계 = scratchpad/as_h1_v2.json (agg_h1_v2.py, 법인별 [qty,amt,cnt]). 이 스크립트는 HTML만 만든다(재실행 가능).
import json, os
from datetime import date

SCR = "C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/42e5fa00-ba61-4d87-863d-64f49b83cc1e/scratchpad/as_h1_v2.json"
d = json.load(open(SCR, encoding="utf-8"))
rows, days = d["rows"], d["days"]

def man(v): return f"{v/10000:,.0f}"
def kg(v):  return f"{v:,.0f}"
def ton(v): return f"{v/1000:,.1f}"

# 총계
cq = sum(r["c"][0] for r in rows); ca = sum(r["c"][1] for r in rows); cc_ = sum(r["c"][2] for r in rows)
wq = sum(r["w"][0] for r in rows); wa = sum(r["w"][1] for r in rows); wc = sum(r["w"][2] for r in rows)
tq, ta = cq + wq, ca + wa

def cell(v, cls=""):  # 0이면 흐리게
    return f'<td class="num {cls}">{v}</td>' if v not in ("0", "0.0") else f'<td class="num zero">–</td>'

tr = []
for i, r in enumerate(rows):
    c, w = r["c"], r["w"]
    tot = r["tot_amt"]
    tr.append(f"""<tr>
      <td class="rank">{i+1}</td><td class="prod">{r['product']}</td>
      {cell(kg(c[0]))}{cell(man(c[1]),'central')}
      {cell(kg(w[0]))}{cell(man(w[1]),'wonhyup')}
      {cell(man(tot),'tot')}
    </tr>""")
rows_html = "\n".join(tr)

html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>수입과일 판매 실적 — 중앙청과·원협 (상반기)</title>
<style>
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Pretendard','Apple SD Gothic Neo',sans-serif; margin:0; background:#f4f6fa; color:#1a2233; }}
  .wrap {{ max-width:960px; margin:0 auto; padding:28px 20px 48px; }}
  header {{ background:#0d47a1; color:#fff; padding:22px 26px; border-radius:12px 12px 0 0; }}
  header h1 {{ margin:0; font-size:21px; letter-spacing:-.5px; }}
  header .sub {{ margin:8px 0 0; color:#cfe0ff; font-size:14px; }}
  .kpis {{ display:flex; gap:12px; padding:18px 26px; background:#fff; border:1px solid #e3e8f0; border-top:none; flex-wrap:wrap; }}
  .kpi {{ flex:1; min-width:150px; text-align:center; padding:13px; background:#f7f9fc; border-radius:10px; border:1px solid #eef1f6; }}
  .kpi .l {{ font-size:12.5px; color:#556; margin-bottom:5px; font-weight:600; }}
  .kpi .v {{ font-size:23px; font-weight:800; letter-spacing:-1px; }}
  .kpi .u {{ font-size:11.5px; color:#7a869a; }}
  .kpi.c .v {{ color:#0d47a1; }} .kpi.w .v {{ color:#c25e00; }} .kpi.t .v {{ color:#1b5e20; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid #e3e8f0; border-top:none;
           border-radius:0 0 12px 12px; overflow:hidden; font-size:13.5px; }}
  thead th {{ background:#e8eef7; color:#243; padding:9px 10px; font-weight:700; border-bottom:2px solid #cdd8e8; font-size:12.5px; }}
  thead .grp-c {{ background:#dce8fb; color:#0d47a1; }} thead .grp-w {{ background:#fbe8d5; color:#c25e00; }}
  td {{ padding:8px 10px; border-bottom:1px solid #eef1f6; }}
  .rank {{ color:#9aa6b8; text-align:center; width:32px; }} .prod {{ font-weight:600; }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .central {{ color:#0d47a1; }} .wonhyup {{ color:#c25e00; }} .tot {{ font-weight:700; color:#1b5e20; }}
  .zero {{ color:#c7cdd6; }}
  tbody tr:nth-child(even) {{ background:#fafbfd; }} tbody tr:hover {{ background:#eef4ff; }}
  tfoot td {{ background:#0d47a1; color:#fff; font-weight:800; padding:11px 10px; font-size:13.5px; }}
  .note {{ margin-top:15px; font-size:12.5px; color:#66738a; line-height:1.7;
           background:#fff; border:1px solid #e3e8f0; border-radius:10px; padding:14px 16px; }}
  .note b {{ color:#0d47a1; }}
  footer {{ margin-top:16px; text-align:right; font-size:11.5px; color:#9aa6b8; }}
  @media print {{ body {{ background:#fff; }} .wrap {{ max-width:100%; }} tbody tr:hover {{ background:none; }} }}
</style></head>
<body><div class="wrap">
  <header>
    <h1>수입과일 판매 실적 — 대전중앙청과(안대명·심세영) · 원협노은</h1>
    <p class="sub">2026년 상반기(1~6월) · 영업일 {days}일 · 두 법인 비교</p>
  </header>
  <div class="kpis">
    <div class="kpi c"><div class="l">중앙청과 (안대명·심세영)</div><div class="v">{man(ca)}</div><div class="u">만원 · {ton(cq)}톤 · {cc_:,}건</div></div>
    <div class="kpi w"><div class="l">원협노은</div><div class="v">{man(wa)}</div><div class="u">만원 · {ton(wq)}톤 · {wc:,}건</div></div>
    <div class="kpi t"><div class="l">합계</div><div class="v">{man(ta)}</div><div class="u">만원 · {ton(tq)}톤</div></div>
  </div>
  <table>
    <thead>
      <tr><th rowspan="2">순위</th><th rowspan="2">품목</th>
          <th colspan="2" class="grp-c">대전중앙청과</th><th colspan="2" class="grp-w">원협노은</th><th rowspan="2">합계<br>금액(만원)</th></tr>
      <tr><th class="grp-c">물량(kg)</th><th class="grp-c">금액(만원)</th><th class="grp-w">물량(kg)</th><th class="grp-w">금액(만원)</th></tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
    <tfoot><tr><td colspan="2">합계</td>
      <td class="num">{kg(cq)}</td><td class="num">{man(ca)}</td>
      <td class="num">{kg(wq)}</td><td class="num">{man(wa)}</td>
      <td class="num">{man(ta)}</td></tr></tfoot>
  </table>
  <div class="note">
    <b>집계 기준</b> · 2026 상반기 정산 데이터(aT API). 품목 = 안대명·심세영 부장 담당 수입과일류(바나나·오렌지·망고·체리·키위·파인애플 등)
    + 다래 + 수입땅콩 + <b>수입곶감·수입호두</b>(2026-07-20 추가). 금액=정산 총액, 물량=정산 총중량.
    <br>※ <b>안대명·심세영은 대전중앙청과 경매사</b>. 원협노은 열은 <b>같은 품목의 원협 실적</b>(원협은 별도 경매사 소관)으로, 두 법인 비교용.
    수입곶감·수입호두는 품종/원산지가 수입인 건만(국산 곶감·호두는 제외).
  </div>
  <footer>제작 터미널: WHOLESALE-T3 · {date.today().isoformat()}</footer>
</div></body></html>"""

out = "C:/Users/samsung/2026/02/monet/presentations/as-fruit-h1-2026-07-20/index.html"
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, "w", encoding="utf-8").write(html)
print("생성:", out)
print(f"중앙 {man(ca)}만원/{ton(cq)}톤 · 원협 {man(wa)}만원/{ton(wq)}톤 · 합계 {man(ta)}만원/{ton(tq)}톤 · {len(rows)}품목")
