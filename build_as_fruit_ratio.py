# 수입과일 상반기 실적 HTML — 중앙 vs 원협 '비율'판 (태은이 2026-07-20).
# 원협 4·5월 = aT유통공사 .xls 실거래로 보정(data.go.kr 계통출하 이중집계 과대 제거).
# 집계 = scratchpad/as_corrected.json (agg_corrected.py). HTML만 생성(재실행 가능).
import json, os
from datetime import date

SCR = "C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/42e5fa00-ba61-4d87-863d-64f49b83cc1e/scratchpad/as_corrected.json"
d = json.load(open(SCR, encoding="utf-8"))
rows, days = d["rows"], d["days"]

def man(v): return f"{v/10000:,.0f}"
def kg(v):  return f"{v:,.0f}"
def ton(v): return f"{v/1000:,.1f}"

cq = sum(r["c"][1] for r in rows); ca = sum(r["c"][0] for r in rows)
wq = sum(r["w"][1] for r in rows); wa = sum(r["w"][0] for r in rows)
tot_a = ca + wa
j_share = ca / tot_a * 100 if tot_a else 0

def bar(ca_, wa_):
    t = ca_ + wa_
    js = ca_ / t * 100 if t else 0
    ws = 100 - js
    return (f'<div class="bar"><span class="bj" style="width:{js:.0f}%">{js:.0f}</span>'
            f'<span class="bw" style="width:{ws:.0f}%">{ws:.0f}</span></div>')

tr = []
for i, r in enumerate(rows):
    c, w = r["c"], r["w"]
    tr.append(f"""<tr>
      <td class="rank">{i+1}</td><td class="prod">{r['product']}</td>
      <td class="num central">{man(c[0])}</td>
      <td class="num wonhyup">{man(w[0])}</td>
      <td class="barcell">{bar(c[0], w[0])}</td>
    </tr>""")
rows_html = "\n".join(tr)

html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>수입과일 상반기 실적 — 중앙청과 vs 원협 (점유율)</title>
<style>
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Pretendard','Apple SD Gothic Neo',sans-serif; margin:0; background:#f4f6fa; color:#1a2233; }}
  .wrap {{ max-width:820px; margin:0 auto; padding:28px 20px 48px; }}
  header {{ background:#0d47a1; color:#fff; padding:22px 26px; border-radius:12px 12px 0 0; }}
  header h1 {{ margin:0; font-size:20px; letter-spacing:-.5px; }}
  header .sub {{ margin:8px 0 0; color:#cfe0ff; font-size:13.5px; }}
  .kpis {{ display:flex; gap:12px; padding:18px 26px; background:#fff; border:1px solid #e3e8f0; border-top:none; }}
  .kpi {{ flex:1; text-align:center; padding:13px; background:#f7f9fc; border-radius:10px; border:1px solid #eef1f6; }}
  .kpi .l {{ font-size:12.5px; color:#556; margin-bottom:5px; font-weight:600; }}
  .kpi .v {{ font-size:24px; font-weight:800; letter-spacing:-1px; }}
  .kpi .u {{ font-size:11.5px; color:#7a869a; }}
  .kpi.c .v {{ color:#0d47a1; }} .kpi.w .v {{ color:#c25e00; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid #e3e8f0; border-top:none;
           border-radius:0 0 12px 12px; overflow:hidden; font-size:13.5px; }}
  thead th {{ background:#e8eef7; color:#243; padding:10px; font-weight:700; border-bottom:2px solid #cdd8e8; font-size:12.5px; }}
  td {{ padding:8px 10px; border-bottom:1px solid #eef1f6; vertical-align:middle; }}
  .rank {{ color:#9aa6b8; text-align:center; width:30px; }} .prod {{ font-weight:600; width:120px; }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums; width:100px; }}
  .central {{ color:#0d47a1; }} .wonhyup {{ color:#c25e00; }}
  .barcell {{ width:220px; }}
  .bar {{ display:flex; height:22px; border-radius:5px; overflow:hidden; font-size:11px; font-weight:700; color:#fff; line-height:22px; }}
  .bj {{ background:#0d47a1; text-align:left; padding-left:6px; min-width:20px; }}
  .bw {{ background:#e08a3c; text-align:right; padding-right:6px; min-width:20px; }}
  tbody tr:nth-child(even) {{ background:#fafbfd; }} tbody tr:hover {{ background:#eef4ff; }}
  tfoot td {{ background:#0d47a1; color:#fff; font-weight:800; padding:11px 10px; }}
  tfoot .bar .bj {{ background:#1b3a6b; }} tfoot .bar .bw {{ background:#b45309; }}
  .legend {{ padding:12px 26px 0; font-size:12.5px; color:#556; }}
  .legend .sw {{ display:inline-block; width:11px; height:11px; border-radius:2px; vertical-align:middle; margin-right:4px; }}
  .note {{ margin-top:15px; font-size:12px; color:#66738a; line-height:1.7;
           background:#fff; border:1px solid #e3e8f0; border-radius:10px; padding:14px 16px; }}
  .note b {{ color:#0d47a1; }} .note .warn {{ color:#b45309; font-weight:600; }}
  footer {{ margin-top:16px; text-align:right; font-size:11.5px; color:#9aa6b8; }}
  @media print {{ body {{ background:#fff; }} .wrap {{ max-width:100%; }} tbody tr:hover {{ background:none; }} }}
</style></head>
<body><div class="wrap">
  <header>
    <h1>수입과일 상반기 판매 점유율 — 대전중앙청과 vs 원협노은</h1>
    <p class="sub">2026년 상반기(1~6월) · 영업일 {days}일 · 원협 4·5월 aT 실거래 보정 반영</p>
  </header>
  <div class="kpis">
    <div class="kpi c"><div class="l">중앙청과 (안대명·심세영)</div><div class="v">{j_share:.1f}%</div><div class="u">{man(ca)}만원 · {ton(cq)}톤</div></div>
    <div class="kpi w"><div class="l">원협노은</div><div class="v">{100-j_share:.1f}%</div><div class="u">{man(wa)}만원 · {ton(wq)}톤</div></div>
  </div>
  <div class="legend"><span class="sw" style="background:#0d47a1"></span>중앙청과 &nbsp;&nbsp;<span class="sw" style="background:#e08a3c"></span>원협노은 &nbsp;·&nbsp; 막대 = 금액 기준 점유율(%)</div>
  <table>
    <thead><tr><th>순위</th><th>품목</th><th>중앙(만원)</th><th>원협(만원)</th><th>점유율 (중앙 : 원협)</th></tr></thead>
    <tbody>
{rows_html}
    </tbody>
    <tfoot><tr><td colspan="2">전체</td><td class="num">{man(ca)}</td><td class="num">{man(wa)}</td><td class="barcell">{bar(ca, wa)}</td></tr></tfoot>
  </table>
  <div class="note">
    <b>집계 기준</b> · 안대명·심세영 부장 담당 수입과일류 + 다래 + 수입땅콩·수입곶감·수입호두. 막대는 <b>금액 기준 점유율(%)</b>.
    <br><span class="warn">★ 원협 4·5월 보정</span> — data.go.kr 아카이브는 원협 4·5월을 계통출하 이중집계로 과대 반영(4월 +47.8%·5월 +20.3%). 이 표는 <b>aT유통공사 도매시장 거래현황(실거래) .xls</b>로 교체한 값. 중앙청과·원협 1·2·3·6월은 아카이브(공식 일치).
    <br>※ 안대명·심세영은 중앙청과 경매사. 원협 열은 같은 품목의 원협 실적(원협은 별도 경매사 소관).
  </div>
  <footer>제작 터미널: WHOLESALE-T3 · {date.today().isoformat()}</footer>
</div></body></html>"""

out = "C:/Users/samsung/2026/02/monet/presentations/as-fruit-h1-2026-07-20/index.html"
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, "w", encoding="utf-8").write(html)
print("생성:", out)
print(f"중앙 {j_share:.1f}% ({man(ca)}만원) · 원협 {100-j_share:.1f}% ({man(wa)}만원) · {len(rows)}품목")
