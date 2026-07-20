# 수입과일 상반기 실적 HTML — 중앙 vs 원협, 물량·금액 + 금액점유율(수치%) (태은이 2026-07-20).
# 원협 4·5월 = aT유통공사 .xls 실거래로 보정(data.go.kr 계통출하 이중집계 과대 제거).
# 집계 = scratchpad/as_corrected.json (agg_as_fruit_corrected.py). HTML만 생성(재실행 가능).
import json, os
from datetime import date

SCR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_as_corrected.json")
d = json.load(open(SCR, encoding="utf-8"))
rows, days = d["rows"], d["days"]

def man(v): return f"{v/10000:,.0f}"
def kg(v):  return f"{v:,.0f}"
def ton(v): return f"{v/1000:,.1f}"

cq = sum(r["c"][1] for r in rows); ca = sum(r["c"][0] for r in rows)
wq = sum(r["w"][1] for r in rows); wa = sum(r["w"][0] for r in rows)
tot_a = ca + wa
j_share = ca / tot_a * 100 if tot_a else 0

tr = []
for i, r in enumerate(rows):
    c, w = r["c"], r["w"]
    t = c[0] + w[0]
    js = c[0] / t * 100 if t else 0
    ws = 100 - js if t else 0
    lead = "j" if js >= 50 else "w"   # 우세 쪽 강조
    tr.append(f"""<tr>
      <td class="rank">{i+1}</td><td class="prod">{r['product']}</td>
      <td class="num central">{kg(c[1])}</td><td class="num central">{man(c[0])}</td>
      <td class="num wonhyup">{kg(w[1])}</td><td class="num wonhyup">{man(w[0])}</td>
      <td class="num share"><b class="s-{lead}">{js:.1f}%</b> : {ws:.1f}%</td>
    </tr>""")
rows_html = "\n".join(tr)

html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>수입과일 상반기 실적 — 중앙청과 vs 원협 (물량·금액·점유율)</title>
<style>
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Pretendard','Apple SD Gothic Neo',sans-serif; margin:0; background:#f4f6fa; color:#1a2233; }}
  .wrap {{ max-width:920px; margin:0 auto; padding:28px 20px 48px; }}
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
           border-radius:0 0 12px 12px; overflow:hidden; font-size:13px; }}
  thead th {{ background:#e8eef7; color:#243; padding:9px 8px; font-weight:700; border-bottom:2px solid #cdd8e8; font-size:12px; }}
  thead .grp-c {{ background:#dce8fb; color:#0d47a1; }} thead .grp-w {{ background:#fbe8d5; color:#c25e00; }}
  td {{ padding:8px; border-bottom:1px solid #eef1f6; }}
  .rank {{ color:#9aa6b8; text-align:center; width:30px; }} .prod {{ font-weight:600; }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .central {{ color:#0d47a1; }} .wonhyup {{ color:#c25e00; }}
  .share {{ width:120px; color:#8a96a8; }}
  .share .s-j {{ color:#0d47a1; }} .share .s-w {{ color:#c25e00; }}
  tbody tr:nth-child(even) {{ background:#fafbfd; }} tbody tr:hover {{ background:#eef4ff; }}
  tfoot td {{ background:#0d47a1; color:#fff; font-weight:800; padding:11px 8px; }}
  tfoot .share {{ color:#cfe0ff; }} tfoot .share b {{ color:#fff; }}
  .note {{ margin-top:15px; font-size:12px; color:#66738a; line-height:1.7;
           background:#fff; border:1px solid #e3e8f0; border-radius:10px; padding:14px 16px; }}
  .note b {{ color:#0d47a1; }} .note .warn {{ color:#b45309; font-weight:600; }}
  footer {{ margin-top:16px; text-align:right; font-size:11.5px; color:#9aa6b8; }}
  @media print {{ body {{ background:#fff; }} .wrap {{ max-width:100%; }} tbody tr:hover {{ background:none; }} }}
</style></head>
<body><div class="wrap">
  <header>
    <h1>수입과일 상반기 실적 — 대전중앙청과 vs 원협노은</h1>
    <p class="sub">2026년 상반기(1~6월) · 영업일 {days}일 · 점유율=금액 기준 · 원협 4·5월 aT 실거래 보정</p>
  </header>
  <div class="kpis">
    <div class="kpi c"><div class="l">중앙청과 (안대명·심세영)</div><div class="v">{j_share:.1f}%</div><div class="u">{man(ca)}만원 · {ton(cq)}톤</div></div>
    <div class="kpi w"><div class="l">원협노은</div><div class="v">{100-j_share:.1f}%</div><div class="u">{man(wa)}만원 · {ton(wq)}톤</div></div>
  </div>
  <table>
    <thead>
      <tr><th rowspan="2">순위</th><th rowspan="2">품목</th>
          <th colspan="2" class="grp-c">대전중앙청과</th><th colspan="2" class="grp-w">원협노은</th>
          <th rowspan="2">금액 점유율<br>(중앙 : 원협)</th></tr>
      <tr><th class="grp-c">물량(kg)</th><th class="grp-c">금액(만원)</th><th class="grp-w">물량(kg)</th><th class="grp-w">금액(만원)</th></tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
    <tfoot><tr><td colspan="2">전체</td>
      <td class="num">{kg(cq)}</td><td class="num">{man(ca)}</td>
      <td class="num">{kg(wq)}</td><td class="num">{man(wa)}</td>
      <td class="num share"><b>{j_share:.1f}%</b> : {100-j_share:.1f}%</td></tr></tfoot>
  </table>
  <div class="note">
    <b>집계 기준</b> · 안대명·심세영 부장 담당 수입과일류 + 다래 + 수입땅콩·수입곶감·수입호두·<b>수입포도·수입블루베리·수입멜론</b>(2026-07-21 추가). <b>점유율은 금액 기준</b>(중앙금액 ÷ 두 법인 합). 수입 판정 = 품종 '(수입)' 또는 원산지 외국(국산 포도·블루베리·멜론은 제외 — 각 원래 경매사 소관).
    <br><span class="warn">★ 원협 4·5월 보정</span> — data.go.kr 아카이브는 원협 4·5월을 계통출하 이중집계로 과대 반영(금액 4월 +47.8%·5월 +20.3%). 이 표는 <b>aT유통공사 도매시장 거래현황(실거래) .xls</b>로 교체. 중앙청과·원협 1·2·3·6월은 아카이브(공식 일치).
    <br>※ 안대명·심세영은 중앙청과 경매사. 원협 열은 같은 품목의 원협 실적(원협은 별도 경매사 소관).
  </div>
  <footer>제작 터미널: WHOLESALE-T3 · {date.today().isoformat()}</footer>
</div></body></html>"""

out = "C:/Users/samsung/2026/02/monet/presentations/as-fruit-h1-2026-07-20/index.html"
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, "w", encoding="utf-8").write(html)
print("생성:", out)
print(f"중앙 {j_share:.1f}% ({man(ca)}만원/{ton(cq)}톤) · 원협 {100-j_share:.1f}% ({man(wa)}만원/{ton(wq)}톤) · {len(rows)}품목")
