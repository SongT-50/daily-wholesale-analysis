"""대전 도매시장 정산 보고서 (기간 + 마지막날 단독 + 전 품목)

month_report.py 양식(라이트 테마, A4 landscape) 기반. 전년 비교 없이 당해 기간만.
구조: ① 마지막 정산일 단독 → ② 기간 누계 4법인 비교 → ③ 시장별 → ④ 전 품목 나열(제한 없음)

사용: python settlement_report.py 2026-05-01 2026-05-23
  python settlement_report.py            → 2026-05 자동 (4법인 모두 정산된 마지막 날까지)
"""
import json, sys, html as html_mod, argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime, date

sys.stdout.reconfigure(encoding='utf-8')

ARCHIVE = Path("C:/Users/samsung/2026/02/wholesale-data")
DAEJEON_CORPS = {
    "25000301": "대전중앙청과㈜", "25000302": "대전원협노은(공)",
    "25000102": "대전청과㈜", "25000101": "농협대전(공)",
}
CORP_ORDER = ["25000301", "25000302", "25000102", "25000101"]
CORP_SHORT = {
    "25000301": "중앙청과", "25000302": "원협노은",
    "25000102": "대전청과", "25000101": "농협대전",
}
NOEUN_CORPS = ["25000301", "25000302"]
OJEONG_CORPS = ["25000102", "25000101"]
MARKET_CODES = ["250001", "250003"]


def load_day(d: date):
    """하루치 4법인 records 로드. 4법인 모두 데이터 있으면 True 동반 반환."""
    f = ARCHIVE / f"{d.year}-{d.month:02d}" / f"auction_{d.isoformat()}.json"
    records = []
    if not f.exists():
        return records, set()
    with open(f, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    corps_present = set()
    for mk in MARKET_CODES:
        if mk in data.get("markets", {}):
            for item in data["markets"][mk].get("items", []):
                if item.get("corp_code") in DAEJEON_CORPS:
                    records.append(item)
                    corps_present.add(item["corp_code"])
    return records, corps_present


def load_range(start: date, end: date):
    """기간 [start, end] 내 4법인 records 전체 + 영업일수."""
    records, days = [], 0
    cur = start
    one = end.toordinal()
    while cur.toordinal() <= one:
        recs, present = load_day(cur)
        if recs:
            days += 1
            records.extend(recs)
        cur = date.fromordinal(cur.toordinal() + 1)
    return records, days


def find_last_settled_day(start: date, end: date):
    """[start, end] 범위에서 4법인 모두 정산된 마지막 날 탐색 (뒤에서부터)."""
    cur = end
    while cur.toordinal() >= start.toordinal():
        _, present = load_day(cur)
        if len(present) == 4:
            return cur
        cur = date.fromordinal(cur.toordinal() - 1)
    return end


def aggregate(records):
    agg = {code: {"qty_kg": 0, "amount": 0, "count": 0} for code in CORP_ORDER}
    for r in records:
        code = r.get("corp_code")
        if code in agg:
            agg[code]["qty_kg"] += r.get("total_qty", 0) or 0
            agg[code]["amount"] += r.get("total_amount", 0) or 0
            agg[code]["count"] += 1
    return agg


def aggregate_by_product(records):
    """전 품목 (제한 없음). 물량 기준 내림차순."""
    product_total = defaultdict(lambda: {"qty_kg": 0, "amount": 0})
    product_corp = defaultdict(lambda: defaultdict(lambda: {"qty_kg": 0, "amount": 0}))
    for r in records:
        code = r.get("corp_code")
        if code not in DAEJEON_CORPS:
            continue
        product = r.get("product", "기타") or "기타"
        qty = r.get("total_qty", 0) or 0
        amt = r.get("total_amount", 0) or 0
        product_total[product]["qty_kg"] += qty
        product_total[product]["amount"] += amt
        product_corp[product][code]["qty_kg"] += qty
        product_corp[product][code]["amount"] += amt
    return sorted(product_total.items(), key=lambda x: x[1]["qty_kg"], reverse=True), product_corp


# === 포맷 ===
def fmt_ton(kg): return f"{kg / 1000:,.1f}"
def fmt_manwon(won): return f"{won / 10000:,.0f}"
def fmt_pct(v): return f"{v:.1f}%"
def fmt_num(n): return f"{n:,}"
def corp_sum(agg, field): return sum(agg.get(c, {field: 0})[field] for c in CORP_ORDER)


def corp_detail_table(agg, title):
    total_qty = corp_sum(agg, "qty_kg") or 1
    total_amt = corp_sum(agg, "amount") or 1
    rows = ""
    for code in CORP_ORDER:
        a = agg[code]
        is_ours = ' class="our-corp"' if code == "25000301" else ""
        rows += f"""<tr{is_ours}>
            <td>{CORP_SHORT[code]}</td>
            <td class="num">{fmt_ton(a['qty_kg'])}</td>
            <td class="num">{fmt_manwon(a['amount'])}</td>
            <td class="num">{fmt_pct(a['qty_kg']/total_qty*100)}</td>
            <td class="num">{fmt_pct(a['amount']/total_amt*100)}</td></tr>"""
    rows += f"""<tr class="total-row">
        <td><strong>합계</strong></td>
        <td class="num"><strong>{fmt_ton(total_qty)}</strong></td>
        <td class="num"><strong>{fmt_manwon(total_amt)}</strong></td>
        <td class="num"><strong>100.0%</strong></td><td class="num"><strong>100.0%</strong></td></tr>"""
    return f"""<h3>{title}</h3>
    <table><thead><tr>
        <th>법인</th><th>물량(톤)</th><th>금액(만원)</th>
        <th>점유율(물량)</th><th>점유율(금액)</th>
    </tr></thead><tbody>{rows}</tbody></table>"""


def two_corp_table(agg, title):
    """대전노은 시장 내 중앙청과 vs 원협노은 직접 비교."""
    c1 = agg.get("25000301", {"qty_kg": 0, "amount": 0})
    c2 = agg.get("25000302", {"qty_kg": 0, "amount": 0})
    tq = (c1["qty_kg"] + c2["qty_kg"]) or 1
    ta = (c1["amount"] + c2["amount"]) or 1
    def ratio(a, b): return f"{a/b:.2f} : 1" if b else "-"
    return f"""<h3>{title}</h3>
    <table><thead><tr>
        <th>항목</th><th class="hl">중앙청과</th><th>원협노은</th><th>비율(중앙:원협)</th>
    </tr></thead><tbody>
        <tr><td>물량(톤)</td><td class="num hl">{fmt_ton(c1['qty_kg'])}</td>
            <td class="num">{fmt_ton(c2['qty_kg'])}</td>
            <td class="num">{ratio(c1['qty_kg'],c2['qty_kg'])}</td></tr>
        <tr><td>금액(만원)</td><td class="num hl">{fmt_manwon(c1['amount'])}</td>
            <td class="num">{fmt_manwon(c2['amount'])}</td>
            <td class="num">{ratio(c1['amount'],c2['amount'])}</td></tr>
        <tr><td>점유율(물량)</td><td class="num hl">{fmt_pct(c1['qty_kg']/tq*100)}</td>
            <td class="num">{fmt_pct(c2['qty_kg']/tq*100)}</td><td class="num">-</td></tr>
        <tr><td>점유율(금액)</td><td class="num hl">{fmt_pct(c1['amount']/ta*100)}</td>
            <td class="num">{fmt_pct(c2['amount']/ta*100)}</td><td class="num">-</td></tr>
    </tbody></table>
    <p class="note">※ 대전노은 시장 내 두 법인 직접 비교 (중앙청과 강조)</p>"""


def market_table(agg):
    nq = sum(agg.get(c, {"qty_kg": 0})["qty_kg"] for c in NOEUN_CORPS)
    na = sum(agg.get(c, {"amount": 0})["amount"] for c in NOEUN_CORPS)
    oq = sum(agg.get(c, {"qty_kg": 0})["qty_kg"] for c in OJEONG_CORPS)
    oa = sum(agg.get(c, {"amount": 0})["amount"] for c in OJEONG_CORPS)
    tq, ta = (nq + oq) or 1, (na + oa) or 1
    rows = f"""<tr><td>대전노은 (중앙청과+원협노은)</td>
        <td class="num">{fmt_ton(nq)}</td><td class="num">{fmt_manwon(na)}</td>
        <td class="num">{fmt_pct(nq/tq*100)}</td><td class="num">{fmt_pct(na/ta*100)}</td></tr>
        <tr><td>대전오정 (대전청과+농협대전)</td>
        <td class="num">{fmt_ton(oq)}</td><td class="num">{fmt_manwon(oa)}</td>
        <td class="num">{fmt_pct(oq/tq*100)}</td><td class="num">{fmt_pct(oa/ta*100)}</td></tr>"""
    return f"""<h3>시장별 비교: 대전노은 vs 대전오정</h3>
    <table><thead><tr><th>시장</th><th>물량(톤)</th><th>금액(만원)</th>
        <th>점유율(물량)</th><th>점유율(금액)</th></tr></thead><tbody>{rows}</tbody></table>"""


def product_table(sorted_products, product_corp):
    rows = ""
    for product, totals in sorted_products:
        total_qty = totals["qty_kg"]
        total_amt = totals["amount"]
        corp_cells = ""
        for code in CORP_ORDER:
            cd = product_corp[product][code]
            pct = (cd["qty_kg"] / total_qty * 100) if total_qty else 0
            hl = ' hl' if code == "25000301" else ""
            bold = " font-weight:600;" if pct >= 40 else ""
            corp_cells += f'<td class="num{hl}" style="{bold}">{fmt_pct(pct)}</td>'
        rows += f"""<tr>
            <td>{html_mod.escape(product)}</td>
            <td class="num">{fmt_ton(total_qty)}</td><td class="num">{fmt_manwon(total_amt)}</td>
            {corp_cells}</tr>"""
    return f"""<h3>품목별 법인 점유율 (전 품목, 물량 내림차순 · 총 {len(sorted_products)}개)</h3>
    <table class="product-table"><thead><tr>
        <th>품목</th><th>물량(톤)</th><th>금액(만원)</th>
        <th class="hl">중앙청과</th><th>원협노은</th><th>대전청과</th><th>농협대전</th>
    </tr></thead><tbody>{rows}</tbody></table>
    <p class="note">※ 점유율 = 대전 4법인 물량 합계 대비. 40% 이상 굵게. 4법인 중 하나라도 정산한 모든 품목 나열.</p>"""


CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap');
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'Noto Sans KR',sans-serif; font-size:12pt; color:#1a1a1a; background:#fff;
       padding:12mm 10mm; line-height:1.5; }
@media print { body{padding:6mm 8mm;font-size:11pt;} .page-break{page-break-before:always;}
       @page{size:A4 landscape;margin:8mm 6mm;} }
@media screen { body{max-width:1200px;margin:0 auto;} .page-break{margin:0;padding:0;height:0;} }
h1 { font-size:20pt; font-weight:700; text-align:center; margin-bottom:2px; color:#0d47a1; }
.subtitle { text-align:center; color:#555; font-size:10pt; margin-bottom:16px; }
h2 { font-size:14pt; font-weight:700; color:#1565c0; border-bottom:2px solid #1565c0;
     padding-bottom:4px; margin:22px 0 10px; }
h3 { font-size:12pt; font-weight:500; color:#333; margin:16px 0 8px; }
table { width:100%; border-collapse:collapse; margin-bottom:16px; font-size:10.5pt; }
th, td { border:1px solid #bbb; padding:5px 8px; text-align:left; }
th { background:#e3f2fd; font-weight:500; text-align:center; white-space:nowrap; font-size:10pt; }
td.num { text-align:right; font-variant-numeric:tabular-nums; }
tr.our-corp { background:#ffe082; font-weight:700; }
tr.our-corp td { font-weight:700; color:#0d47a1; border-top:1.5px solid #f9a825; border-bottom:1.5px solid #f9a825; }
th.hl { background:#fff3c4; color:#0d47a1; }
td.hl { background:#fffdf0; font-weight:600; }
tr.total-row { background:#f0f0f0; border-top:2px solid #888; }
.product-table th, .product-table td { padding:4px 6px; font-size:10pt; }
.note { font-size:9pt; color:#777; margin-top:4px; }
.footer { margin-top:24px; padding-top:8px; border-top:1px solid #ddd;
          font-size:9pt; color:#999; text-align:center; }
</style>"""


def generate_html(start, end, last_day, range_agg, day_agg, product_data, days):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"{start.year}년 {start.month}월 대전 도매시장 정산 보고서"
    period = f"{start.year}.{start.month:02d}.{start.day:02d} ~ {end.year}.{end.month:02d}.{end.day:02d}"
    last_label = f"{last_day.month}월 {last_day.day}일({'월화수목금토일'[last_day.weekday()]})"

    page = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>{title}</title>{CSS}</head><body>

<h1>{title}</h1>
<p class="subtitle">기간: {period} (정산 완료일 기준) | 영업 {days}일 | 생성: {now}<br>
출처: 농산물유통정보(aT) 정산정보 API | 4법인: 대전중앙청과·원협노은·대전청과·농협대전</p>
<p class="note" style="text-align:center;color:#d32f2f;">
※ 공판장(원협노은·농협대전) 정산 2~3일 지연 → 4법인 모두 정산 완료된 마지막 날({last_label})까지 집계</p>

<h2>1. 마지막 정산일 ({last_label}) 현황</h2>
{corp_detail_table(day_agg, f"{last_label} 4법인 정산 (당일)")}

<h2>2. {start.month}월 누계 ({period})</h2>
{corp_detail_table(range_agg, f"{start.month}.1~{end.month}.{end.day} 누계 ({days}일)")}
{two_corp_table(range_agg, "중앙청과 vs 원협노은 (대전노은 시장)")}
{market_table(range_agg)}

<div class="page-break"></div>
<h2>3. 품목별 정산 현황 ({start.month}월 누계, 전 품목)</h2>
{product_table(*product_data)}

<div class="footer">
    대전중앙청과 경영기획 | 출처: 농산물유통정보(aT) 정산정보 API | 공판장 정산 2~3일 지연 가능
</div>
</body></html>"""
    return page


def main():
    parser = argparse.ArgumentParser(description="대전 도매시장 정산 보고서 (기간 + 마지막날 + 전품목)")
    parser.add_argument("start", nargs="?", default="2026-05-01")
    parser.add_argument("end", nargs="?", default=None, help="미지정 시 4법인 모두 정산된 마지막 날 자동 탐색")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    # end 미지정 시: 해당 월 말일까지 탐색 범위로 두고 마지막 정산일 자동 검색
    if args.end:
        end = date.fromisoformat(args.end)
    else:
        # 같은 달 25일 정도까지 탐색 후 마지막 정산일
        probe_end = date(start.year, start.month, 28)
        end = find_last_settled_day(start, probe_end)

    last_day = find_last_settled_day(start, end)

    print("=" * 60)
    print(f"  {start.isoformat()} ~ {end.isoformat()} 정산 보고서")
    print(f"  마지막 정산일(4법인 완비): {last_day.isoformat()}")
    print("=" * 60)

    range_records, days = load_range(start, end)
    range_agg = aggregate(range_records)
    day_records, _ = load_day(last_day)
    day_agg = aggregate(day_records)
    product_data = aggregate_by_product(range_records)

    rq = corp_sum(range_agg, "qty_kg"); ra = corp_sum(range_agg, "amount")
    print(f"\n누계: {days}일, {len(range_records):,}건, {rq/1000:,.1f}톤, {ra/10000:,.0f}만원")
    print(f"품목: {len(product_data[0])}개")
    print(f"마지막날({last_day}): {corp_sum(day_agg,'qty_kg')/1000:,.1f}톤, {corp_sum(day_agg,'amount')/10000:,.0f}만원")

    html_content = generate_html(start, end, last_day, range_agg, day_agg, product_data, days)
    out = Path(f"C:/Users/samsung/2026/02/monet/daily-wholesale-analysis/settlement_report_{start.year}-{start.month:02d}.html")
    out.write_text(html_content, encoding="utf-8")
    print(f"\n{out}")


if __name__ == "__main__":
    main()
