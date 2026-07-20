# 안대명·심세영 부장 상반기(2026 1~6월) 수입과일 판매 실적 HTML 생성기
# 데이터 = 대전중앙청과(25000301) 기준, 안대명·심세영 담당 품목(수입과일15+다래+수입땅콩).
# 집계 산출 = scratchpad/as_h1.json (agg_h1.py). 이 스크립트는 그 JSON을 읽어 HTML만 만든다(재실행 가능).
import json, sys, os
from datetime import date
sys.stdout.reconfigure(encoding="utf-8")

SCR = "C:/Users/samsung/AppData/Local/Temp/claude/C--Users-samsung-2026-02-monet/42e5fa00-ba61-4d87-863d-64f49b83cc1e/scratchpad/as_h1.json"
d = json.load(open(SCR, encoding="utf-8"))
rows, tq, ta, tc, days = d["rows"], d["tq"], d["ta"], d["tc"], d["days"]

def won(v):  return f"{v:,.0f}"
def man(v):  return f"{v/10000:,.0f}"
def kg(v):   return f"{v:,.0f}"
def ton(v):  return f"{v/1000:,.1f}"

tr = []
for i, r in enumerate(rows):
    share = r["amt"] / ta * 100 if ta else 0
    tr.append(f"""<tr>
      <td class="rank">{i+1}</td>
      <td class="prod">{r['product']}</td>
      <td class="num">{kg(r['qty'])}</td>
      <td class="num">{man(r['amt'])}</td>
      <td class="num">{r['cnt']:,}</td>
      <td class="num share">{share:.1f}%</td>
    </tr>""")
rows_html = "\n".join(tr)

html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>안대명·심세영 상반기 수입과일 판매 실적</title>
<style>
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.css');
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Pretendard','Apple SD Gothic Neo',sans-serif; margin:0; background:#f4f6fa; color:#1a2233; }}
  .wrap {{ max-width:900px; margin:0 auto; padding:28px 24px 48px; }}
  header {{ background:#0d47a1; color:#fff; padding:22px 26px; border-radius:12px 12px 0 0; }}
  header h1 {{ margin:0; font-size:22px; letter-spacing:-.5px; }}
  header .sub {{ margin:8px 0 0; color:#cfe0ff; font-size:14px; }}
  .kpis {{ display:flex; gap:14px; padding:20px 26px; background:#fff; border:1px solid #e3e8f0; border-top:none; }}
  .kpi {{ flex:1; text-align:center; padding:14px; background:#f7f9fc; border-radius:10px; border:1px solid #eef1f6; }}
  .kpi .v {{ font-size:26px; font-weight:800; color:#0d47a1; letter-spacing:-1px; }}
  .kpi .u {{ font-size:12px; color:#7a869a; }}
  .kpi .l {{ font-size:13px; color:#556; margin-bottom:6px; font-weight:600; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid #e3e8f0; border-top:none;
           border-radius:0 0 12px 12px; overflow:hidden; font-size:14px; }}
  thead th {{ background:#e8eef7; color:#243; padding:11px 12px; font-weight:700; border-bottom:2px solid #cdd8e8; }}
  td {{ padding:10px 12px; border-bottom:1px solid #eef1f6; }}
  .rank {{ color:#9aa6b8; text-align:center; width:36px; }}
  .prod {{ font-weight:600; }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .share {{ color:#7a869a; width:70px; }}
  tbody tr:nth-child(even) {{ background:#fafbfd; }}
  tbody tr:hover {{ background:#eef4ff; }}
  tfoot td {{ background:#0d47a1; color:#fff; font-weight:800; padding:13px 12px; font-size:15px; }}
  tfoot .num {{ font-size:16px; }}
  .note {{ margin-top:16px; font-size:12.5px; color:#66738a; line-height:1.7;
           background:#fff; border:1px solid #e3e8f0; border-radius:10px; padding:14px 16px; }}
  .note b {{ color:#0d47a1; }}
  footer {{ margin-top:18px; text-align:right; font-size:11.5px; color:#9aa6b8; }}
  @media print {{ body {{ background:#fff; }} .wrap {{ max-width:100%; }} tbody tr:hover {{ background:none; }} }}
</style></head>
<body><div class="wrap">
  <header>
    <h1>수입과일 판매 실적 — 안대명·심세영 부장</h1>
    <p class="sub">대전중앙청과㈜ · 2026년 상반기(1~6월) · 영업일 {days}일</p>
  </header>
  <div class="kpis">
    <div class="kpi"><div class="l">총 물량</div><div class="v">{ton(tq)}</div><div class="u">톤 ({kg(tq)} kg)</div></div>
    <div class="kpi"><div class="l">총 금액</div><div class="v">{man(ta)}</div><div class="u">만원 ({won(ta)} 원)</div></div>
    <div class="kpi"><div class="l">거래 건수</div><div class="v">{tc:,}</div><div class="u">건 · {len(rows)}개 품목</div></div>
  </div>
  <table>
    <thead><tr><th>순위</th><th>품목</th><th>물량(kg)</th><th>금액(만원)</th><th>건수</th><th>금액비중</th></tr></thead>
    <tbody>
{rows_html}
    </tbody>
    <tfoot><tr><td colspan="2">합계</td><td class="num">{kg(tq)}</td><td class="num">{man(ta)}</td><td class="num">{tc:,}</td><td class="num">100%</td></tr></tfoot>
  </table>
  <div class="note">
    <b>집계 기준</b> · 대전중앙청과㈜ 정산 데이터(농산물유통정보 aT API) 중 안대명·심세영 부장 담당 품목만.
    담당 품목 = 수입과일류(바나나·오렌지·망고·체리·참다래(키위)·파인애플·아보카도·레몬·용과·망고스턴·자몽·듀리안·코코넛·아로니아·탄제린)
    + 다래 + 수입땅콩(원산지 외국·품종 '수입'). 아래 표는 상반기 실제 거래분(15품목). 금액=정산 총액, 물량=정산 총중량.
    <br>※ 경매사별 표는 <b>중앙청과 기준</b>(2026-06-01 결재) — 타 법인 수입과일은 각 법인 경매사 소관이라 제외.
  </div>
  <footer>제작 터미널: WHOLESALE-T3 · {date.today().isoformat()}</footer>
</div></body></html>"""

out = "C:/Users/samsung/2026/02/monet/presentations/as-fruit-h1-2026-07-20/index.html"
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, "w", encoding="utf-8").write(html)
print("생성:", out)
print(f"총 {ton(tq)}톤 / {man(ta)}만원 / {tc}건 / {len(rows)}품목")
