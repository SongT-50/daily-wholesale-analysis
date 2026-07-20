# -*- coding: utf-8 -*-
"""상반기 경매사별 실적 — 대전중앙청과 노은 (올해 vs 작년) 자동 생성기.

  python build_auctioneer_halfyear.py            # 2026 vs 2025 상반기(1~6월)
  python build_auctioneer_halfyear.py --check     # 총계만 월계표 대조(HTML 안 씀)

★ 재구축 배경(2026-07-20 WHOLESALE-T3): 이 보고서는 그동안 정적 DATA 배열을 손으로 만들어
   경매사 배정이 바뀌면 반영이 안 됐다. 이제 아래를 자동화한다.
 - base 경매사별 = 로컬 아카이브 load_range + agg_auctioneer → 현재 AUCTION_BLOCKS 배정 자동 반영
   (settlement_report/build_noeun_report 재사용 = 정산메일·노은보고서와 동일 로직 = 수치 정합)
 - 소실 배분 = corrections/source/데이터누락일자자료_2026H1.xlsx 품목을 현재 AUCTION_BLOCKS로 매핑
   (apply_missing_corrections.py 로직 포팅 — 배정 변경이 소실 배분에도 자동 반영)
 - 화면 CSS/레이아웃/JS 렌더러는 기존 손튜닝본 유지, DATA·헤드라인만 재계산 주입.

정직 표기(data-to-claim):
 - aT 원천에 없는 중앙 소실일(작년 1/3·1/4·1/7·1/9=27.27억/912톤, 올해 2/24=5.08억/245톤)은
   회사 정산 자료로만 실제값 확보 → 품목 매핑으로 경매사 배분(특수품목 일부는 '소실 미배분').
 - 총계는 회사 월계표(2026 835.7억/38,241톤, 2025 881.4억/37,101톤)와 대조해 검증.
"""
import sys, os, io, json, glob, re, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
HERE = os.path.dirname(os.path.abspath(__file__))
import settlement_report as sr
import build_noeun_report as bn
from datetime import date
from collections import defaultdict, Counter
import openpyxl

J = "25000301"                          # 중앙청과 노은
ARCH = os.getenv("AUCTION_ARCHIVE_DIR", "C:/Users/samsung/2026/02/wholesale-data")
XLSX = os.path.join(HERE, 'corrections', 'source', '데이터누락일자자료_2026H1.xlsx')
CORR = json.load(open(os.path.join(HERE, 'corrections', 'missing_corrections.json'), encoding='utf-8'))
MDAYS = {k: v for k, v in CORR['missing_days'].items() if not k.startswith('_')}
LOSS_SHEETS = {2025: ['25.1.3', '25.1.4', '25.1.7', '25.1.9'], 2026: ['26.2.24']}
LEDGER = {2026: (835.7, 38241), 2025: (881.4, 37101)}   # 회사 월계표 공식 총계(억, 톤) — 검증 앵커
YCUR, YPREV = 2026, 2025

# clean_label(AUCTION_BLOCKS[3]) → (표시명, 담당 품목 예시). 없으면 clean_label + top품목 자동.
NICE = {
    "송화신 이사":            ("송화신 이사", "버섯류·고추류·파프리카"),
    "(서병수, 김선우) 부장":  ("서병수·김선우 부장", "엽채·양채류(상추·시금치·쌈배추 등)"),
    "강신창 부장":            ("강신창 부장", "근채·양채류"),
    "김기영 부장":            ("김기영 부장", "무·대파·배추(쌈배추 제외)·양배추"),
    "김언중 부장":            ("김언중 부장", "열무·쪽파·옥수수·알타리·얼갈이·갓·실파"),
    "이용수 부장":            ("이용수 부장", "당근·양파"),
    "오준서 경매사":          ("오준서 경매사", "마늘·생강·건고추·연근·숙주 등"),
    "이기송 부장":            ("이기송 부장", "복숭아·자두·딸기·토마토·국산땅콩"),
    "(김상걸, 차수호) 이사":  ("김상걸·차수호 이사", "감·포도·참외·곶감"),
    "윤정기 이사":            ("윤정기 이사", "수박·감귤·만감"),
    "이광진 부장":            ("이광진 부장", "배·사과"),
    "(안대명, 심세영) 부장 (수입과일)": ("안대명·심세영 부장", "수입과일"),
    "나머지 (수삼·약용·메밀)": ("나머지", "수삼·약용·메밀"),
    "소실 미배분":            ("소실 미배분", "허브·두부·묵류 등(aT 미등록 특수품목)"),
    "미배정":                 ("미배정", "분류 전(신규·희소 품목)"),
}
FAINT = {"나머지", "소실 미배분", "미배정"}


def base_year(year):
    """중앙(J) 상반기 경매사 라벨별 집계 (현재 AUCTION_BLOCKS 배정 반영).
    영업일 = J(중앙 노은)가 실제 정산된 날 수 = load_range의 '4법인 아무나' 카운트와 다름
    (소실일은 J=0이라도 타 법인 존재로 잡히므로 J별도 카운트)."""
    from datetime import timedelta
    jrec, jdays = [], 0
    cur, end = date(year, 1, 1), date(year, 6, 30)
    while cur <= end:
        recs, _ = sr.load_day(cur)
        jr = [r for r in recs if r.get('corp_code') == J]
        if jr:
            jdays += 1
            jrec += jr
        cur += timedelta(days=1)
    data, order, prods = bn.agg_auctioneer(jrec)
    return data, order, prods, jdays


# ── 소실 배분 (apply_missing_corrections 로직 포팅, 재실행 가능하게 커밋본 xlsx 사용) ──
def build_prod2cat():
    tmp = defaultdict(Counter)
    for y in (YPREV, YCUR):
        for m in range(1, 7):
            for p in glob.glob(f'{ARCH}/{y}-{m:02d}/auction_*.json'):
                if p.endswith('.bak') or os.path.getsize(p) < 300_000:
                    continue
                try:
                    d = json.load(open(p, encoding='utf-8'))
                except Exception:
                    continue
                for it in d.get('markets', {}).get('250003', {}).get('items', []) or []:
                    if it.get('corp_code') == J:
                        pr = it.get('product'); cc = it.get('category_code')
                        if pr:
                            tmp[pr][cc] += 1
    return {k: v.most_common(1)[0][0] for k, v in tmp.items()}


MANUAL = {'감': '단감', '파': '대파', '피망': '피망(단고추)', '나물': '취나물', '로즈': '로즈마리'}


def make_mapp(prod2cat):
    products = sorted(prod2cat, key=len, reverse=True)

    def mapp(name):
        name = name.strip()
        if name in prod2cat:
            return name
        if name in MANUAL and MANUAL[name] in prod2cat:
            return MANUAL[name]
        cands = [p for p in products if p.startswith(name)]
        if cands:
            return cands[0]
        cands = [p for p in products if name.startswith(p)]
        if cands:
            return cands[0]
        return None
    return mapp


def loss_by_label(prod2cat, mapp):
    """연도별 {clean_label: [amount, qty]} 소실 배분."""
    wb = openpyxl.load_workbook(XLSX, data_only=True)
    out = {}
    for year, sheets in LOSS_SHEETS.items():
        auc = defaultdict(lambda: [0.0, 0.0])
        for sh in sheets:
            for row in wb[sh].iter_rows(min_row=2, values_only=True):
                p = row[0]
                if p is None:
                    continue
                p = str(p).strip()
                if p in ('-소계-', '=합계='):
                    continue
                q = float(row[2] or 0); a = float(row[3] or 0)
                mp = mapp(p)
                if mp:
                    idx = sr.auction_block_index(mp, prod2cat[mp])
                    lab = sr.AUCTION_BLOCKS[idx][3] if idx < len(sr.AUCTION_BLOCKS) else '소실 미배분'
                    lab = bn.clean_label(lab)
                else:
                    lab = '소실 미배분'
                auc[lab][0] += a; auc[lab][1] += q
        out[year] = dict(auc)
    return out


def compute():
    d_cur, o_cur, p_cur, days_cur = base_year(YCUR)
    d_prev, o_prev, p_prev, days_prev = base_year(YPREV)
    prod2cat = build_prod2cat()
    loss = loss_by_label(prod2cat, make_mapp(prod2cat))

    # base 라벨(raw) → clean 라벨로 접기 (agg는 raw 라벨 키). 소실은 clean 키.
    def fold(data):
        out = defaultdict(lambda: [0.0, 0.0]); order = {}
        for lb, v in data.items():
            cl = bn.clean_label(lb)
            out[cl][0] += v[J][1]  # amount
            out[cl][1] += v[J][0]  # qty
            order.setdefault(cl, 900)
        return out, order

    b_cur, _ = fold(d_cur); b_prev, _ = fold(d_prev)
    # 표시 순서 = AUCTION_BLOCKS 등장 순 (raw 라벨 첫 등장 index)
    seq = []
    for b in sr.AUCTION_BLOCKS:
        cl = bn.clean_label(b[3])
        if cl not in seq:
            seq.append(cl)
    for extra in ("소실 미배분", "미배정"):
        if extra not in seq:
            seq.append(extra)

    labels = set(b_cur) | set(b_prev) | set(loss[YCUR]) | set(loss[YPREV])
    rows = []
    for cl in sorted(labels, key=lambda x: (seq.index(x) if x in seq else 999)):
        ca = (b_cur.get(cl, [0, 0])[0] + loss[YCUR].get(cl, [0, 0])[0]) / 1e8
        pa = (b_prev.get(cl, [0, 0])[0] + loss[YPREV].get(cl, [0, 0])[0]) / 1e8
        cq = (b_cur.get(cl, [0, 0])[1] + loss[YCUR].get(cl, [0, 0])[1]) / 1000
        pq = (b_prev.get(cl, [0, 0])[1] + loss[YPREV].get(cl, [0, 0])[1]) / 1000
        if abs(ca) + abs(pa) < 1e-9:
            continue
        nm, desc = NICE.get(cl, (cl, "—"))
        rows.append({'n': nm, 'p': desc, 'ca': round(ca, 2), 'pa': round(pa, 2),
                     'cq': round(cq), 'pq': round(pq), 'faint': 1 if cl in FAINT else 0})
    return rows, days_cur, days_prev


def _arrow(d, unit, pct=None):
    """헤드라인용 화살표 span. 증가=초록▲ / 감소=빨강▼."""
    cls = 'up' if d >= 0 else 'down'
    sym = '▲ +' if d >= 0 else '▼ −'
    s = f'<span class="{cls}">{sym}{abs(d):,.1f}{unit}'
    if pct is not None:
        s += f' ({"+" if pct >= 0 else "−"}{abs(pct):.1f}%)'
    return s + '</span>'


def build_html(rows, days_cur, days_prev):
    import datetime as _dt
    sca = sum(r['ca'] for r in rows); spa = sum(r['pa'] for r in rows)
    scq = sum(r['cq'] for r in rows); spq = sum(r['pq'] for r in rows)
    amt_d = sca - spa; amt_pct = amt_d / spa * 100 if spa else 0
    qty_d = scq - spq; qty_pct = qty_d / spq * 100 if spq else 0
    up_cur = (sca * 1e8) / (scq * 1000) if scq else 0       # 원/kg
    up_prev = (spa * 1e8) / (spq * 1000) if spq else 0
    up_d_pct = (up_cur - up_prev) / up_prev * 100 if up_prev else 0

    real = [r for r in rows if not r['faint']]
    top_up = max(real, key=lambda r: r['ca'] - r['pa'])
    top_dn = min(real, key=lambda r: r['ca'] - r['pa'])

    data_js = "\n".join(
        ' {' + f'n:"{r["n"]}", p:"{r["p"]}", ca:{r["ca"]}, pa:{r["pa"]}, cq:{r["cq"]}, pq:{r["pq"]}'
        + (', faint:1' if r['faint'] else '') + '},'
        for r in rows)

    # 소실일 표
    loss_prev = [(k, v) for k, v in sorted(MDAYS.items()) if k.startswith(f"{YPREV}-")]
    loss_cur = [(k, v) for k, v in sorted(MDAYS.items()) if k.startswith(f"{YCUR}-")]
    lpa = sum(v['amount'] for _, v in loss_prev) / 1e8
    lpq = sum(v['qty_kg'] for _, v in loss_prev) / 1000
    lca = sum(v['amount'] for _, v in loss_cur) / 1e8
    lcq = sum(v['qty_kg'] for _, v in loss_cur) / 1000
    WD = ['월', '화', '수', '목', '금', '토', '일']

    def drow(k, v):
        dt = _dt.date.fromisoformat(k)
        yl = "작년" if dt.year == YPREV else "올해"
        return (f'<td>{yl} {dt.month}/{dt.day} ({WD[dt.weekday()]})</td>'
                f'<td class="cur">{v["amount"]/1e8:.2f}억</td><td>{v["qty_kg"]/1000:.0f}톤</td>')

    n = len(loss_prev)
    loss_html = ""
    for i, (k, v) in enumerate(loss_prev):
        rs = (f'<td rowspan="{n+1}" style="vertical-align:middle;color:#667;text-align:center">'
              f'회사<br>정산자료</td>') if i == 0 else ""
        loss_html += f'      <tr>{drow(k, v)}{rs}</tr>\n'
    loss_html += (f'      <tr style="background:#eef3f8;font-weight:800"><td>작년 소계</td>'
                  f'<td class="cur">{lpa:.2f}억</td><td>{lpq:.0f}톤</td></tr>\n')
    for k, v in loss_cur:
        loss_html += (f'      <tr style="border-top:2px solid #ccc">{drow(k, v)}'
                      f'<td style="color:#667;text-align:center">회사 정산</td></tr>\n')

    today = _dt.date.today().isoformat()
    rep = {
        "YCUR": str(YCUR), "YPREV": str(YPREV), "TODAY": today,
        "CUR_AMT": f"{sca:.1f}", "PREV_AMT": f"{spa:.1f}", "AMT_ARROW": _arrow(amt_d, "억", amt_pct),
        "CUR_QTY": f"{int(round(scq)):,}", "PREV_QTY": f"{int(round(spq)):,}", "QTY_ARROW": _arrow(qty_d, "톤", qty_pct),
        "CUR_UP": f"{up_cur:,.0f}", "PREV_UP": f"{up_prev:,.0f}",
        "UP_ARROW": f'<span class="{"up" if up_d_pct>=0 else "down"}">{"▲" if up_d_pct>=0 else "▼"} {"+" if up_d_pct>=0 else "−"}{abs(up_d_pct):.1f}%</span>',
        "LOSS_PREV_A": f"{lpa:.2f}", "LOSS_PREV_Q": f"{lpq:.0f}",
        "LOSS_CUR_A": f"{lca:.2f}", "LOSS_CUR_Q": f"{lcq:.0f}",
        "TOP_UP_NAME": top_up['n'], "TOP_UP_D": f"{top_up['ca']-top_up['pa']:+.1f}", "TOP_UP_P": top_up['p'],
        "TOP_DN_NAME": top_dn['n'], "TOP_DN_D": f"{top_dn['ca']-top_dn['pa']:+.1f}", "TOP_DN_P": top_dn['p'],
        "QTY_PCT": f"{abs(qty_pct):.1f}", "QTY_WORD": "늘었고" if qty_d >= 0 else "줄었고",
        "UP_PCT": f"{abs(up_d_pct):.1f}", "UP_WORD": "올라" if up_d_pct >= 0 else "떨어져",
        "AMT_PCT": f"{abs(amt_pct):.1f}", "AMT_WORD": "증가" if amt_d >= 0 else "감소",
        "AMT_ARROW_SHORT": f'{"+" if amt_d>=0 else "−"}{abs(amt_d):.1f}억',
        "DAYS_CUR": str(days_cur), "DAYS_PREV": str(days_prev),
        "LEDGER_CUR": f"{LEDGER[YCUR][0]:.1f}", "LEDGER_PREV": f"{LEDGER[YPREV][0]:.1f}",
        "DATA_JS": data_js, "LOSS_HTML": loss_html.rstrip("\n"),
    }
    tpl = open(os.path.join(HERE, "_auct_template.html"), encoding="utf-8").read()
    for k, v in rep.items():
        tpl = tpl.replace(f"@@{k}@@", v)
    left = re.findall(r"@@[A-Z_]+@@", tpl)
    if left:
        raise SystemExit(f"미치환 placeholder 남음: {sorted(set(left))}")
    return tpl


CACHE = None  # set in __main__


def _write_html(rows, days_cur, days_prev):
    html = build_html(rows, days_cur, days_prev)
    out_dir = os.path.join("C:/Users/samsung/2026/02/monet", "presentations", "auctioneer-halfyear-2025vs2026")
    os.makedirs(out_dir, exist_ok=True)
    for fn in ("print.html", "경매사별 상반기 실적 (올해 vs 작년, 실제값).html"):
        open(os.path.join(out_dir, fn), "w", encoding="utf-8").write(html)
    open(os.path.join(HERE, "_auct_saved.txt"), "w", encoding="utf-8").write(out_dir + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="총계만 월계표 대조(HTML 안 씀)")
    ap.add_argument("--render", action="store_true",
                    help="느린 집계 생략, _auct_cache.json으로 HTML만 재생성(템플릿 수정용)")
    args = ap.parse_args()

    cache_path = os.path.join(HERE, "_auct_cache.json")
    if args.render:
        c = json.load(open(cache_path, encoding="utf-8"))
        _write_html(c["rows"], c["days_cur"], c["days_prev"])
        raise SystemExit(0)

    rows, days_cur, days_prev = compute()
    json.dump({"rows": rows, "days_cur": days_cur, "days_prev": days_prev},
              open(cache_path, "w", encoding="utf-8"), ensure_ascii=False)
    sca = sum(r['ca'] for r in rows); spa = sum(r['pa'] for r in rows)
    scq = sum(r['cq'] for r in rows); spq = sum(r['pq'] for r in rows)
    lc = LEDGER[YCUR]; lp = LEDGER[YPREV]
    log = []
    log.append(f"[총계 대조] {YCUR} {sca:.1f}억/{scq:,.0f}톤  (월계표 {lc[0]}억/{lc[1]:,}톤)")
    log.append(f"           {YPREV} {spa:.1f}억/{spq:,.0f}톤  (월계표 {lp[0]}억/{lp[1]:,}톤)")
    for r in rows:
        log.append(f"  {r['n']:18s} 올해 {r['ca']:7.1f}억 {r['cq']:>6}톤 | 작년 {r['pa']:7.1f}억 {r['pq']:>6}톤  Δ{r['ca']-r['pa']:+6.1f}")
    open(os.path.join(HERE, "_auct_check.txt"), "w", encoding="utf-8").write("\n".join(log) + "\n")

    if not args.check:
        _write_html(rows, days_cur, days_prev)
