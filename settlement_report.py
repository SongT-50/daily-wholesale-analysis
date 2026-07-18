"""대전 도매시장 정산 보고서 (기간 + 마지막날 단독 + 전 품목)

month_report.py 양식(라이트 테마, A4 landscape) 기반. 전년 비교 없이 당해 기간만.
구조: ① 마지막 정산일 단독 → ② 기간 누계 4법인 비교 → ③ 시장별 → ④ 전 품목 나열(제한 없음)

사용: python settlement_report.py 2026-05-01 2026-05-23
  python settlement_report.py            → 2026-05 자동 (4법인 모두 정산된 마지막 날까지)
"""
import os, json, sys, html as html_mod, argparse, calendar
from pathlib import Path
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone

sys.stdout.reconfigure(encoding='utf-8')

# 아카이브 경로: 로컬은 월별 하위폴더({YYYY-MM}/), 클라우드(Actions)는 레포 data/ flat 구조.
# AUCTION_ARCHIVE_DIR 환경변수로 오버라이드 (Actions에서 data/ 지정). 기본값 = 로컬 아카이브.
ARCHIVE = Path(os.getenv("AUCTION_ARCHIVE_DIR", "C:/Users/samsung/2026/02/wholesale-data"))
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

# 품목별 표 표시 순서 = 경매 진행 순서 (태은이 5/30 결재 — 경매시간·경매사 순서).
# 부류번호·데이터는 그대로 유지하고 "표시 순서"만 재배치한다.
# 각 블록 = (부류코드, 포함품목 frozenset|None=전체, 제외품목 frozenset|None, 경매사라벨).
# 블록 순서 = 표시 순서. 블록 내부는 물량(qty_kg) 내림차순.
# 경매사 라벨이 바뀌면 표에 경매사 구분행 삽입. 여러 명은 "(이름, 이름)" 형식.
AUCTION_BLOCKS = [
    # ── 🥬 채소 파트 ──
    # 태은이 2026-07-18 결재: 송화신 이사 = 시간대(00:00 버섯·엽채 / 01:10 고추·파프리카) 나누지 말고
    #   하나로 합쳐 맨 앞칸에. 라벨을 '송화신 이사'로 통일 → 첫 블록(여기)이라 정렬 순서 0 = 첫칸.
    ("17", None, None, "송화신 이사"),
    ("10", None, frozenset({"갓", "배추", "숙주나물", "우엉대", "콩나물",
                            "토란대", "얼갈이배추", "열무", "양배추"}), "00:20 (서병수, 김선우) 부장"),
    ("11", frozenset({"삼채"}), None, "00:20 (서병수, 김선우) 부장"),
    ("12", frozenset({"겨자"}), None, "00:20 (서병수, 김선우) 부장"),
    ("13", None, frozenset({"파프리카", "피망(단고추)"}), "00:20 (서병수, 김선우) 부장"),
    ("14", None, None, "00:20 (서병수, 김선우) 부장"),
    ("03", None, None, "00:20 (서병수, 김선우) 부장"),
    ("09", None, None, "00:30 강신창 부장"),
    ("12", frozenset({"꽈리고추", "풋고추", "홍고추"}), None, "송화신 이사"),
    ("13", frozenset({"파프리카", "피망(단고추)"}), None, "송화신 이사"),
    # 태은이 2026-07-06 결재: 김기영·김언중 공동 → 품목별 분리 (앞으로 계속 적용)
    #   김기영 = 배추·무·대파·양배추 / 김언중 = 열무·쪽파·옥수수·알타리·얼갈이·갓·실파
    ("10", frozenset({"배추", "양배추"}), None, "김기영 부장"),
    ("11", frozenset({"무"}), None, "김기영 부장"),
    ("12", frozenset({"대파"}), None, "김기영 부장"),
    ("10", frozenset({"얼갈이배추", "열무", "갓"}), None, "김언중 부장"),
    ("11", frozenset({"알타리무"}), None, "김언중 부장"),
    ("12", frozenset({"실파", "쪽파"}), None, "김언중 부장"),
    ("04", frozenset({"옥수수"}), None, "김언중 부장"),
    ("05", None, None, "이용수 부장"),
    ("11", frozenset({"당근"}), None, "이용수 부장"),
    ("12", frozenset({"양파"}), None, "이용수 부장"),
    ("10", frozenset({"숙주나물", "우엉대", "콩나물", "토란대"}), None, "오준서 경매사"),
    ("11", frozenset({"연근", "우엉", "토란"}), None, "오준서 경매사"),
    ("12", frozenset({"마늘", "생강", "건고추"}), None, "오준서 경매사"),
    ("04", frozenset({"기장"}), None, "오준서 경매사"),
    ("91", None, None, "오준서 경매사"),
    # ── 🍎 과일 파트 ──
    ("06", frozenset({"매실", "복숭아", "보리수", "블루베리", "살구", "앵두", "오디", "자두"}), None, "04:30 이기송 부장"),
    ("07", frozenset({"대추", "밤", "잣"}), None, "04:30 이기송 부장"),
    ("08", frozenset({"딸기", "멜론", "방울토마토", "토마토"}), None, "04:30 이기송 부장"),
    ("06", frozenset({"곶감", "단감", "떫은감", "포도"}), None, "(김상걸, 차수호) 이사"),
    ("08", frozenset({"참외"}), None, "(김상걸, 차수호) 이사"),
    ("06", frozenset({"감귤", "만감"}), None, "윤정기 이사"),
    ("08", frozenset({"수박"}), None, "윤정기 이사"),
    ("06", frozenset({"배", "사과"}), None, "이광진 부장"),
    ("06", frozenset({"듀리안", "레몬", "망고", "망고스턴", "바나나", "아로니아",
                      "아보카도", "오렌지", "용과", "자몽", "참다래(키위)", "체리",
                      "코코넛", "탄제린", "파인애플"}), None, "(안대명, 심세영) 부장 (수입과일·수입땅콩)"),
    ("07", frozenset({"다래"}), None, "(안대명, 심세영) 부장 (수입과일·수입땅콩)"),
    # 태은이 2026-07-18 결재: 수입 땅콩 → 안대명·심세영 (수입과일과 같은 경매사).
    #   대전 4법인 수입 땅콩은 전량 중국산·품종 '땅콩(수입)'으로 라벨 일치 → 국산 땅콩과 안전 분리.
    #   판정은 load_day에서 수입 레코드의 product를 '수입땅콩'으로 재라벨(아래 _reclassify_import).
    ("16", frozenset({"수입땅콩"}), None, "(안대명, 심세영) 부장 (수입과일·수입땅콩)"),
    # 태은이 2026-07-18 결재: 국산 땅콩 → 이기송 부장 (앞으로 계속). 수입은 위 안대명·심세영.
    ("16", frozenset({"땅콩"}), None, "04:30 이기송 부장"),
    ("04", frozenset({"메밀"}), None, "나머지 (수삼·약용·메밀)"),
    ("16", None, None, "나머지 (수삼·약용·메밀)"),
    ("18", None, None, "나머지 (수삼·약용·메밀)"),
    ("19", None, None, "나머지 (수삼·약용·메밀)"),
]
_AUCTION_FALLBACK = len(AUCTION_BLOCKS) + 1
# 과일 파트 시작 블록 인덱스 (채소엔 부류 06 없음 → 첫 06 블록이 과일 파트 시작점).
FRUIT_START_BLOCK = next(i for i, b in enumerate(AUCTION_BLOCKS) if b[0] == "06")

# 경매사 라벨이 처음 등장하는 블록 인덱스 → 같은 경매사 = 한 덩어리로 묶기 위한 순서표.
# (태은이 5/30 방식2 결재: 한 경매사가 여러 부류를 다뤄도 부류 무시하고 그 경매사 품목 전체를 금액순으로)
_LABEL_ORDER = {}
for _i, _b in enumerate(AUCTION_BLOCKS):
    if _b[3] not in _LABEL_ORDER:
        _LABEL_ORDER[_b[3]] = _i

# 법인(공판장)별 입력 단위·방식이 상이하여 물량/비율 비교 시 주의가 필요한 품목 → * 표시.
# (배추류 = 공판장 입력 단위·방식 상이 / 보리수 = 국산·수입 혼입 가능, 수입 품목과 겹침 주의)
STAR_ITEMS = frozenset({"배추", "얼갈이배추", "열무", "실파", "쪽파", "보리수"})


def auction_block_index(product, category_code):
    """품목이 속하는 경매 블록 인덱스(표시 순서). 매칭 없으면 맨 뒤(_AUCTION_FALLBACK)."""
    for i, (cc, include, exclude, _label) in enumerate(AUCTION_BLOCKS):
        if cc != category_code:
            continue
        if include is not None and product not in include:
            continue
        if exclude is not None and product in exclude:
            continue
        return i
    return _AUCTION_FALLBACK


def auction_label_order(product, category_code):
    """품목이 속한 경매사 그룹의 정렬 순서값 (같은 경매사 = 같은 값 = 한 덩어리).
    미배정 품목은 맨 뒤(_AUCTION_FALLBACK)."""
    bidx = auction_block_index(product, category_code)
    if bidx >= len(AUCTION_BLOCKS):
        return _AUCTION_FALLBACK
    return _LABEL_ORDER[AUCTION_BLOCKS[bidx][3]]


# 수입 땅콩 판정 (태은이 2026-07-18): 품종에 '수입' 또는 원산지가 외국이면 수입.
#   대전 4법인은 두 신호가 일치(수입=전량 중국산·품종 '땅콩(수입)')라 오분류 위험 낮음.
#   ※ 국산 땅콩(품종 일반/피땅콩/깐땅콩, 원산지 국내)은 재라벨 안 함 → product='땅콩' 유지
#     → AUCTION_BLOCKS의 ("16",{"땅콩"}) 블록으로 이기송 부장 배정(태은이 2026-07-18).
_IMPORT_ORIGIN_KEYS = ("중국", "미국", "수입", "태국", "인도", "베트남",
                       "아르헨", "브라질", "남아", "호주", "볼리비아", "기타국가")


def _reclassify_import(item: dict) -> dict:
    """수입 땅콩이면 product를 '수입땅콩'으로 바꾼 사본 반환(원본 불변). 아니면 그대로."""
    if (item.get("product") or "") != "땅콩":
        return item
    variety = item.get("variety") or ""
    origin = item.get("origin") or ""
    if "수입" in variety or any(k in origin for k in _IMPORT_ORIGIN_KEYS):
        return {**item, "product": "수입땅콩"}
    return item


def load_day(d: date):
    """하루치 4법인 records 로드. 정산한 법인 집합 동반 반환.
    월별 하위폴더 구조(로컬)와 flat 구조(Actions data/) 모두 탐색."""
    candidates = [
        ARCHIVE / f"{d.year}-{d.month:02d}" / f"auction_{d.isoformat()}.json",
        ARCHIVE / f"auction_{d.isoformat()}.json",
    ]
    f = next((c for c in candidates if c.exists()), None)
    records = []
    if f is None:
        return records, set()
    with open(f, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    corps_present = set()
    for mk in MARKET_CODES:
        if mk in data.get("markets", {}):
            for item in data["markets"][mk].get("items", []):
                if item.get("corp_code") in DAEJEON_CORPS:
                    records.append(_reclassify_import(item))
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


# 공판장(원협노은·농협대전) 정산 지연 → 오늘로부터 N일 지난 날까지만 집계해
# 미완성(절반짜리) 수치가 자동 메일로 나가는 것을 방지.
# 태은이 결재(2026-05-29): 3일 → 갱신(2026-07-07): 4일.
#   근거: 모든 법인은 정산자료를 D+4는 지나야 완비된다(태은이 도메인 재확인). find_last_settled_day는
#   4법인 '존재'만 확인하므로(부분데이터 오판 여지), cutoff를 D+4로 늦춰 완비 확실성을 높인다.
SETTLE_LAG_DAYS = int(os.getenv("SETTLE_LAG_DAYS", "4"))


def resolve_auto_end(start: date, today: date = None):
    """자동 모드 종료일: min(월말, 오늘-LAG)까지 탐색 → 4법인 완비된 마지막 정산일.
    예) 오늘 5/29 · LAG 3 → 5/26까지만 본다 (D+3 지나 백필 안정된 자료만)."""
    # Actions runner는 UTC → KST(+9) 기준 날짜로 환산해야 "오늘"이 한국 날짜와 일치
    today = today or datetime.now(timezone(timedelta(hours=9))).date()
    last_dom = calendar.monthrange(start.year, start.month)[1]
    cutoff = today - timedelta(days=SETTLE_LAG_DAYS)
    search_end = min(date(start.year, start.month, last_dom), cutoff)
    if search_end < start:
        search_end = start
    return find_last_settled_day(start, search_end)


def resolve_report_range(today: date = None):
    """자동 모드 (start, end) 한 쌍. 보고서 '기준 월'을 오늘이 아니라
    안정화 기준일(오늘 - LAG)의 월로 잡는다.

    공판장(원협노은·농협대전)이 2~3일 늦게 올리므로, 달이 막 바뀐 직후
    (예: 6/1~6/3)에는 새 달 데이터가 아직 D+LAG를 안 지나 미완성이다.
    이때 '어제 기준 월'로 잡으면 새 달 첫날(미완비)을 보고서로 만들어
    버리는 월 경계 버그가 난다(2026-06-02 메일: 마지막 정산일 6/1·4법인 미완비).
    → anchor = 오늘 - LAG 의 월을 기준으로 삼아, 새 달 데이터가 익기 전엔
    전월 누계를 유지하고, D+LAG가 지나면(예: 6/4~) 자동으로 새 달로 전환한다.
    예) 오늘 6/2 · LAG 3 → anchor 5/30 → 5월 누계(~5월 완비 마지막날).
        오늘 6/4 · LAG 3 → anchor 6/1 → 6월 누계(6/1~)."""
    today = today or datetime.now(timezone(timedelta(hours=9))).date()
    anchor = today - timedelta(days=SETTLE_LAG_DAYS)
    start = date(anchor.year, anchor.month, 1)
    end = resolve_auto_end(start, today)
    return start, end


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
    """전 품목 (제한 없음). 부류코드(2자리) 오름차순 → 품목명 가나다순 정렬.
    정산자료엔 부류코드(category_code)만 있고 품목 4자리 코드는 없음 → 부류코드순으로 우선 정렬.
    품목별 4법인 물량·금액을 모두 보관(중앙청과·원협노은 실수치 표기에 사용)."""
    product_total = defaultdict(lambda: {"qty_kg": 0, "amount": 0})
    product_corp = defaultdict(lambda: defaultdict(lambda: {"qty_kg": 0, "amount": 0}))
    product_cat = {}  # product -> (category_code, category_name) 첫 등장 부류 유지
    for r in records:
        code = r.get("corp_code")
        if code not in DAEJEON_CORPS:
            continue
        product = r.get("product", "기타") or "기타"
        cc = (r.get("category_code") or "").strip()
        cn = (r.get("category") or "").strip()
        if product not in product_cat:
            product_cat[product] = (cc, cn)
        qty = r.get("total_qty", 0) or 0
        amt = r.get("total_amount", 0) or 0
        product_total[product]["qty_kg"] += qty
        product_total[product]["amount"] += amt
        product_corp[product][code]["qty_kg"] += qty
        product_corp[product][code]["amount"] += amt
    # 경매사 그룹 순서(시간순) + 같은 경매사 안에서는 부류 무시하고 중앙청과 금액 내림차순.
    # (2026-06-01 태은이 결재: 표의 표시 금액=중앙청과 기준과 정렬을 일치시킴.
    #  5/30 방식2의 '4법인 합계' → '중앙청과'로 변경. 중앙청과 취급 없는 품목(중앙 0)은
    #  4법인 합계 금액으로 2차 정렬해 그룹 내 순서를 안정적으로 유지.)
    JUNG_CORP = "25000301"  # 대전중앙청과
    def _jung_amt(p):
        return product_corp.get(p, {}).get(JUNG_CORP, {}).get("amount", 0)
    sorted_products = sorted(
        product_total.items(),
        key=lambda x: (auction_label_order(x[0], product_cat[x[0]][0]),
                       -_jung_amt(x[0]), -x[1]["amount"]),
    )
    return sorted_products, product_corp, product_cat


# === 포맷 ===
def fmt_ton(kg): return f"{kg / 1000:,.1f}"
def fmt_manwon(won): return f"{won / 10000:,.0f}"
def fmt_pct(v): return f"{v:.1f}%"
def fmt_num(n): return f"{n:,}"
def corp_sum(agg, field): return sum(agg.get(c, {field: 0})[field] for c in CORP_ORDER)


def ratio_pct(a, b):
    """대전노은 두 법인(중앙청과:원협노은) 비중을 합 100 기준 정수쌍으로. 예: 55:45.
    둘 다 0이면 '-'. 반올림 오차는 큰 쪽에 흡수시켜 합이 항상 100이 되게 한다."""
    t = (a or 0) + (b or 0)
    if t <= 0:
        return "-"
    pa = round(a / t * 100)
    pb = 100 - pa
    return f"{pa}:{pb}"


def validate_data(range_agg, product_data, last_day, range_records):
    """데이터 정합성 검증. 문제 발견 시 경고 문자열 리스트 반환 (빈 리스트 = 통과).
    검증 항목: ①마지막 정산일 4법인 완비 ②기간 데이터 존재 ③품목합=법인합(물량·금액)."""
    warnings = []

    # ① 마지막 정산일 4법인 완비 여부
    _, present = load_day(last_day)
    if len(present) < 4:
        missing = [CORP_SHORT[c] for c in CORP_ORDER if c not in present]
        warnings.append(f"마지막 정산일({last_day.isoformat()}) 4법인 미완비 — 미정산: {', '.join(missing)}")

    # ② 기간 내 데이터 존재
    if not range_records:
        warnings.append("기간 내 정산 데이터 0건 — 데이터 누락 의심")
        return warnings

    # ③ 품목별 합산 = 법인별 합산 (독립 재집계 교차검증)
    sorted_products = product_data[0]
    prod_qty = sum(t["qty_kg"] for _, t in sorted_products)
    prod_amt = sum(t["amount"] for _, t in sorted_products)
    corp_qty = corp_sum(range_agg, "qty_kg")
    corp_amt = corp_sum(range_agg, "amount")
    if abs(prod_qty - corp_qty) > 1:  # kg 반올림 오차 허용
        warnings.append(f"물량 집계 불일치 — 품목합 {prod_qty/1000:,.1f}톤 ≠ 법인합 {corp_qty/1000:,.1f}톤")
    if abs(prod_amt - corp_amt) > 1:
        warnings.append(f"금액 집계 불일치 — 품목합 {prod_amt/10000:,.0f}만원 ≠ 법인합 {corp_amt/10000:,.0f}만원")

    # ④ 경매 블록 미배정(신규·계절 품목) 검사 — 계절에 새로 나온 품목이 경매사 지정에서 누락되면 경고.
    #    (부류06·07·08·11·12처럼 품목을 경매사별로 나눈 부류에 새 품목이 나오면 여기에 잡힘)
    product_cat = product_data[2]
    unassigned = [p for p, _ in sorted_products
                  if auction_block_index(p, product_cat.get(p, ("", ""))[0]) == _AUCTION_FALLBACK]
    if unassigned:
        warnings.append(
            f"경매사 미배정 신규·계절 품목 {len(unassigned)}개: {', '.join(unassigned)} "
            f"— settlement_report.py AUCTION_BLOCKS에 해당 품목의 경매사·위치 지정 필요 "
            f"(표 맨 끝 '🆕 미배정' 줄에 임시 배치됨)")

    return warnings


def validation_box(warnings):
    """검증 결과 HTML 박스 (경고 있으면 빨강, 없으면 초록)."""
    if warnings:
        items = "".join(f"<li>{html_mod.escape(w)}</li>" for w in warnings)
        return (f'<div style="background:#fff3f3;border:1.5px solid #d32f2f;border-radius:6px;'
                f'padding:10px 14px;margin:10px 0;color:#b71c1c;font-size:10.5pt;">'
                f'<strong>⚠️ 데이터 검증 경고 {len(warnings)}건</strong> (받아보고 확인 요망)'
                f'<ul style="margin:6px 0 0 18px;">{items}</ul></div>')
    return ('<div style="background:#f1f8f1;border:1.5px solid #2e7d32;border-radius:6px;'
            'padding:8px 14px;margin:10px 0;color:#1b5e20;font-size:10.5pt;">'
            '✅ 데이터 검증 통과 — 4법인 완비 · 품목합=법인합 (물량·금액 정합)</div>')


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
    return f"""<h3>{title}</h3>
    <table><thead><tr>
        <th>항목</th><th class="hl">중앙청과</th><th>원협노은</th>
    </tr></thead><tbody>
        <tr><td>물량(톤)</td><td class="num hl">{fmt_ton(c1['qty_kg'])}</td>
            <td class="num">{fmt_ton(c2['qty_kg'])}</td></tr>
        <tr><td>금액(만원)</td><td class="num hl">{fmt_manwon(c1['amount'])}</td>
            <td class="num">{fmt_manwon(c2['amount'])}</td></tr>
        <tr><td>점유율(물량)</td><td class="num hl">{fmt_pct(c1['qty_kg']/tq*100)}</td>
            <td class="num">{fmt_pct(c2['qty_kg']/tq*100)}</td></tr>
        <tr><td>점유율(금액)</td><td class="num hl">{fmt_pct(c1['amount']/ta*100)}</td>
            <td class="num">{fmt_pct(c2['amount']/ta*100)}</td></tr>
    </tbody></table>
    <p class="note">※ 대전노은 시장 내 두 법인 직접 비교 (중앙청과 강조). 점유율 = 두 법인 합 100 기준</p>"""


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


def _auc_subtotal_row(auc_label, sj_a, sj_q, sw_a, sw_q):
    """경매사 블록 소계 행 HTML."""
    return (
        f'<tr class="auc-subtotal">'
        f'<td class="cat" colspan="2">소계 ({html_mod.escape(auc_label)})</td>'
        f'<td class="num hl">{fmt_manwon(sj_a)}</td><td class="num hl">{fmt_ton(sj_q)}</td>'
        f'<td class="num won">{fmt_manwon(sw_a)}</td><td class="num won">{fmt_ton(sw_q)}</td>'
        f'<td class="num rt">{ratio_pct(sj_a, sw_a)}</td>'
        f'<td class="num rt">{ratio_pct(sj_q, sw_q)}</td>'
        f'<td colspan="8" class="num" style="color:#999;font-size:8.5pt;text-align:center;">—</td>'
        f'</tr>'
    )


def product_table(sorted_products, product_corp, product_cat):
    """품목별 표 (경매 진행 순서, 블록 내부 금액순). 중앙청과·원협노은은 금액(만원)·물량(톤) 실수치 +
    중앙:원협 비율(금액·물량, 55:45 형태). 4법인 전체는 물량 점유율(%)로 표기.
    (태은이 5/30: 금액·물량 순서 + 경매사 블록 내부 금액순 / 6/19: 경매사 블록 소계 행)
    (태은이 7/1: 품목 16개 이상 경매사 블록은 물량 많은 순 상위 15개만 표시 + 나머지 '기타'로 합산.
     서병수·김선우 부장(엽채류 76품목)·오준서 경매사 등 압축. 소계·합계는 전 품목 반영해 정합 유지.)"""
    J, W, DJ, NH = "25000301", "25000302", "25000102", "25000101"
    ETC_LIMIT = 15
    UNASSIGNED = "🆕 미배정 (신규·계절 품목 — 경매사 지정 필요)"

    # 경매사별 그룹화 (sorted_products가 이미 경매 순서 → 첫 등장 순서 유지)
    groups = []
    gi = {}
    for product, totals in sorted_products:
        cc, _ = product_cat.get(product, ("", ""))
        bidx = auction_block_index(product, cc)
        part = "fruit" if bidx >= FRUIT_START_BLOCK else "veg"
        auc = AUCTION_BLOCKS[bidx][3] if bidx < len(AUCTION_BLOCKS) else UNASSIGNED
        if auc not in gi:
            gi[auc] = len(groups)
            groups.append([auc, part, []])
        groups[gi[auc]][2].append((product, totals, cc))

    def prod_row(product, totals, cc):
        tq = totals["qty_kg"]; ta = totals["amount"]
        j = product_corp[product][J]; w = product_corp[product][W]
        dj = product_corp[product][DJ]; nh = product_corp[product][NH]
        pq = lambda d: (d["qty_kg"] / tq * 100) if tq else 0
        pa = lambda d: (d["amount"] / ta * 100) if ta else 0
        return f"""<tr>
            <td class="cat">{html_mod.escape(cc)}</td>
            <td>{html_mod.escape(product)}{'<span class="star">*</span>' if product in STAR_ITEMS else ''}</td>
            <td class="num hl">{fmt_manwon(j['amount'])}</td><td class="num hl">{fmt_ton(j['qty_kg'])}</td>
            <td class="num won">{fmt_manwon(w['amount'])}</td><td class="num won">{fmt_ton(w['qty_kg'])}</td>
            <td class="num rt">{ratio_pct(j['amount'], w['amount'])}</td>
            <td class="num rt">{ratio_pct(j['qty_kg'], w['qty_kg'])}</td>
            <td class="num hl">{fmt_pct(pa(j))}</td><td class="num won">{fmt_pct(pa(w))}</td>
            <td class="num">{fmt_pct(pa(dj))}</td><td class="num">{fmt_pct(pa(nh))}</td>
            <td class="num hl">{fmt_pct(pq(j))}</td><td class="num won">{fmt_pct(pq(w))}</td>
            <td class="num">{fmt_pct(pq(dj))}</td><td class="num">{fmt_pct(pq(nh))}</td></tr>"""

    rows = ""; prev_part = None; shown = 0
    for auc, part, items in groups:
        if part != prev_part:
            plabel = "🍎 과일 파트 (04:30~)" if part == "fruit" else "🥬 채소 파트 (00:00~)"
            rows += f'<tr class="part-divider"><td colspan="16">{plabel}</td></tr>'
            prev_part = part
        ac = "auc-row unassigned" if "미배정" in auc else "auc-row"
        rows += f'<tr class="{ac}"><td colspan="16">↳ {html_mod.escape(auc)}</td></tr>'

        # 품목 16개 이상 = 물량 많은 순 상위 15개 + 나머지 '기타' 합산
        if len(items) > ETC_LIMIT:
            items_sorted = sorted(items, key=lambda x: -x[1]["qty_kg"])
            show, etc = items_sorted[:ETC_LIMIT], items_sorted[ETC_LIMIT:]
        else:
            show, etc = items, []

        sj_a = sj_q = sw_a = sw_q = 0
        for product, totals, cc in show:
            rows += prod_row(product, totals, cc)
            sj_a += product_corp[product][J]['amount']; sj_q += product_corp[product][J]['qty_kg']
            sw_a += product_corp[product][W]['amount']; sw_q += product_corp[product][W]['qty_kg']
            shown += 1

        if etc:
            agg = {c: {'amount': 0.0, 'qty_kg': 0.0} for c in (J, W, DJ, NH)}
            etq = eta = 0.0
            for product, totals, cc in etc:
                etq += totals['qty_kg']; eta += totals['amount']
                for c in (J, W, DJ, NH):
                    agg[c]['amount'] += product_corp[product][c]['amount']
                    agg[c]['qty_kg'] += product_corp[product][c]['qty_kg']
            sj_a += agg[J]['amount']; sj_q += agg[J]['qty_kg']
            sw_a += agg[W]['amount']; sw_q += agg[W]['qty_kg']
            epq = lambda d: (d['qty_kg'] / etq * 100) if etq else 0
            epa = lambda d: (d['amount'] / eta * 100) if eta else 0
            rows += f"""<tr style="color:#555;background:#fafafa;">
            <td class="cat">—</td>
            <td><em>기타 ({len(etc)}개 품목 합산)</em></td>
            <td class="num hl">{fmt_manwon(agg[J]['amount'])}</td><td class="num hl">{fmt_ton(agg[J]['qty_kg'])}</td>
            <td class="num won">{fmt_manwon(agg[W]['amount'])}</td><td class="num won">{fmt_ton(agg[W]['qty_kg'])}</td>
            <td class="num rt">{ratio_pct(agg[J]['amount'], agg[W]['amount'])}</td>
            <td class="num rt">{ratio_pct(agg[J]['qty_kg'], agg[W]['qty_kg'])}</td>
            <td class="num hl">{fmt_pct(epa(agg[J]))}</td><td class="num won">{fmt_pct(epa(agg[W]))}</td>
            <td class="num">{fmt_pct(epa(agg[DJ]))}</td><td class="num">{fmt_pct(epa(agg[NH]))}</td>
            <td class="num hl">{fmt_pct(epq(agg[J]))}</td><td class="num won">{fmt_pct(epq(agg[W]))}</td>
            <td class="num">{fmt_pct(epq(agg[DJ]))}</td><td class="num">{fmt_pct(epq(agg[NH]))}</td></tr>"""

        rows += _auc_subtotal_row(auc, sj_a, sj_q, sw_a, sw_q)

    total_prods = len(sorted_products)
    return f"""<h3>품목별 정산 (경매 진행 순서 · 표시 {shown}개 + 기타 합산 / 전 품목 {total_prods}개) — 중앙청과·원협노은 강조</h3>
    <table class="product-table"><thead>
    <tr>
        <th rowspan="2">부류</th><th rowspan="2">품목</th>
        <th colspan="2" class="hl">중앙청과 ★</th>
        <th colspan="2" class="won-h">원협노은</th>
        <th colspan="2">중앙:원협 비율</th>
        <th colspan="4">금액 점유율 (4법인)</th>
        <th colspan="4">물량 점유율 (4법인)</th>
    </tr>
    <tr>
        <th class="hl">금액(만원)</th><th class="hl">물량(톤)</th>
        <th class="won-h">금액(만원)</th><th class="won-h">물량(톤)</th>
        <th>금액</th><th>물량</th>
        <th class="hl">중앙</th><th class="won-h">원협</th><th>대전청과</th><th>농협대전</th>
        <th class="hl">중앙</th><th class="won-h">원협</th><th>대전청과</th><th>농협대전</th>
    </tr></thead><tbody>{rows}</tbody></table>
    <p class="note">※ 경매 진행 순서(경매사별, 채소 → 과일)로 배열. <strong>품목 16개 이상 경매사는 물량 많은 순 상위 15개만 표시하고 나머지는 '기타'로 합산</strong>(소계·합계는 전 품목 반영). 그 외 경매사 블록 내부는 금액 많은 순.
    중앙청과·원협노은은 금액·물량 실수치 + 두 법인 비율(금액·물량, 합 100, 예 55:45). 금액 점유율 = 4법인 금액 합계 대비 / 물량 점유율 = 4법인 물량 합계 대비 각 법인 비중.<br>
    <span class="star">*</span> 표시 품목 — 배추·얼갈이배추·열무·실파·쪽파: <strong>법인(공판장)별 입력 단위·방식이 상이</strong> / 보리수: <strong>국산·수입 혼입 가능(수입 품목과 겹침)</strong> → 물량·비율 직접 비교 시 주의 요망.</p>"""


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
.product-table th, .product-table td { padding:4px 6px; font-size:9.5pt; }
td.cat { text-align:center; color:#777; font-variant-numeric:tabular-nums; }
th.won-h { background:#e8f5e9; color:#1b5e20; }
td.won { background:#f4faf4; }
td.rt { text-align:center; font-weight:600; color:#444; font-variant-numeric:tabular-nums; }
tr.part-divider td { background:#1565c0; color:#fff; font-weight:700; font-size:11pt;
                     text-align:center; padding:7px; letter-spacing:1px; }
tr.auc-row td { background:#eef4fb; color:#0d47a1; font-weight:600; font-size:9.5pt;
                text-align:left; padding:4px 10px; border-top:1.5px solid #90caf9; }
tr.auc-row.unassigned td { background:#fff3f3; color:#b71c1c; border-top:1.5px solid #d32f2f; }
tr.auc-subtotal td { background:#dce9f7; font-weight:700; border-top:2px solid #4a90d9;
                     border-bottom:2px solid #4a90d9; font-size:9.5pt; }
tr.auc-subtotal td.hl { background:#c8daf0; }
tr.auc-subtotal td.won { background:#d4e9d8; }
.star { color:#d32f2f; font-weight:700; }
.note { font-size:9pt; color:#777; margin-top:4px; }
.footer { margin-top:24px; padding-top:8px; border-top:1px solid #ddd;
          font-size:9pt; color:#999; text-align:center; }
</style>"""


def generate_html(start, end, last_day, range_agg, day_agg, product_data, days, daily=False, warnings=None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    period = f"{start.year}.{start.month:02d}.{start.day:02d} ~ {end.year}.{end.month:02d}.{end.day:02d}"
    last_label = f"{last_day.month}월 {last_day.day}일({'월화수목금토일'[last_day.weekday()]})"

    if daily:
        # 하루치 단독본: 누계 = 당일이라 누계 섹션 생략, 전부 당일 기준
        title = f"{last_day.year}년 {last_label} 대전 도매시장 정산 보고서"
        subtitle = (f"정산일: {last_label} (하루치 단독) | 생성: {now}<br>"
                    "출처: 농산물유통정보(aT) 정산정보 API | 4법인: 대전중앙청과·원협노은·대전청과·농협대전")
        body = f"""
<h2>1. {last_label} 4법인 정산 현황</h2>
{corp_detail_table(day_agg, f"{last_label} 4법인 정산")}
{two_corp_table(day_agg, "중앙청과 vs 원협노은 (대전노은 시장)")}
{market_table(day_agg)}

<div class="page-break"></div>
<h2>2. 품목별 정산 현황 ({last_label}, 전 품목)</h2>
{product_table(*product_data)}"""
    else:
        title = f"{start.year}년 {start.month}월 대전 도매시장 정산 보고서"
        subtitle = (f"기간: {period} (정산 완료일 기준) | 영업 {days}일 | 생성: {now}<br>"
                    "출처: 농산물유통정보(aT) 정산정보 API | 4법인: 대전중앙청과·원협노은·대전청과·농협대전")
        # 공판장 5/1~5/8 정산 자료 중복 이관 오류 주의 (태은이 도메인 확인, 2026-06-04).
        # 누계 기간이 2026-05-01~05-08과 겹칠 때만 표시 (6월 등 다른 달엔 미표시).
        dupe_warn = ""
        if start <= date(2026, 5, 8) and end >= date(2026, 5, 1):
            dupe_warn = (
                '<div style="background:#fff3f3;border:2px solid #d32f2f;border-radius:6px;'
                'padding:12px 16px;margin:12px 0;color:#b71c1c;font-size:11pt;line-height:1.65;">'
                '<strong>⚠️ 5월 누계 데이터 주의 — 공판장 정산 자료 중복 오류</strong><br>'
                '공판장에서 <strong>2026년 5월 1일 ~ 5월 8일</strong> 기간의 정산 자료를 '
                '<strong>중복 이관</strong>하여, 해당 기간 4법인 물량·금액이 실제보다 '
                '<strong>과대 계상</strong>되어 있습니다. 아래 5월 누계 수치는 이 점을 감안하여 '
                '<strong>참고용</strong>으로 확인 바랍니다.</div>')
        body = f"""<p class="note" style="text-align:center;color:#d32f2f;">
※ 공판장(원협노은·농협대전) 정산 2~3일 지연 → 4법인 모두 정산 완료된 마지막 날({last_label})까지 집계</p>

<h2>1. 마지막 정산일 ({last_label}) 현황</h2>
{corp_detail_table(day_agg, f"{last_label} 4법인 정산 (당일)")}

<h2>2. {start.month}월 누계 ({period})</h2>
{dupe_warn}
{corp_detail_table(range_agg, f"{start.month}.1~{end.month}.{end.day} 누계 ({days}일)")}
{two_corp_table(range_agg, "중앙청과 vs 원협노은 (대전노은 시장)")}
{market_table(range_agg)}

<div class="page-break"></div>
<h2>3. 품목별 정산 현황 ({start.month}월 누계, 전 품목)</h2>
{product_table(*product_data)}"""

    page = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>{title}</title>{CSS}</head><body>

<h1>{title}</h1>
<p class="subtitle">{subtitle}</p>
{validation_box(warnings or [])}
{body}

<div class="footer">
    대전중앙청과 경영기획 | 출처: 농산물유통정보(aT) 정산정보 API | 공판장 정산 2~3일 지연 가능
</div>
</body></html>"""
    return page


BASE_OUT = Path(os.getenv("SETTLEMENT_OUT_DIR", "C:/Users/samsung/2026/02/monet/daily-wholesale-analysis"))


def _resolve_out(out_arg, default_name):
    if out_arg:
        return Path(out_arg) if Path(out_arg).is_absolute() else BASE_OUT / out_arg
    return BASE_OUT / default_name


def build_report(start: date, end: date, out_arg=None, force_last_day=None):
    """[start, end] 정산 보고서 1개 생성. stats dict 반환 (메일 본문 요약용).

    force_last_day 지정 시 4법인 완비 검증(find_last_settled_day)을 건너뛰고
    그 날을 마지막 정산일로 강제한다. 공휴일 등으로 특정 법인이 휴장해
    4법인 완비가 영영 불가능한 날을 수동으로 보고할 때 사용 (예: 2026-06-06
    현충일에 대전청과 미영업 → 3법인으로 6/6 보고). 자동 모드는 항상 None."""
    last_day = force_last_day or find_last_settled_day(start, end)
    daily = (start == end)
    range_records, days = load_range(start, end)
    range_agg = aggregate(range_records)
    day_records, _ = load_day(last_day)
    day_agg = aggregate(day_records)
    product_data = aggregate_by_product(day_records if daily else range_records)
    warnings = validate_data(range_agg, product_data, last_day, range_records)

    html_content = generate_html(start, end, last_day, range_agg, day_agg,
                                 product_data, days, daily=daily, warnings=warnings)
    default_name = (f"settlement_report_{last_day.isoformat()}_daily.html" if daily
                    else f"settlement_report_{start.isoformat()}_to_{end.isoformat()}.html")
    out = _resolve_out(out_arg, default_name)
    out.write_text(html_content, encoding="utf-8")

    rq, ra = corp_sum(range_agg, "qty_kg"), corp_sum(range_agg, "amount")
    stats = {"kind": "하루치" if daily else "누계", "daily": daily,
             "start": start, "end": end, "last_day": last_day, "days": days,
             "records": len(range_records), "qty_kg": rq, "amount": ra,
             "products": len(product_data[0]), "warnings": warnings, "path": out}
    print(f"[{stats['kind']}] {start.isoformat()}~{end.isoformat()} | 마지막정산일 {last_day.isoformat()} | "
          f"{days}일 {len(range_records):,}건 {rq/1000:,.1f}톤 {ra/10000:,.0f}만원 | "
          f"품목 {len(product_data[0])} | 검증경고 {len(warnings)}건")
    for w in warnings:
        print(f"   ⚠️ {w}")
    print(f"   -> {out}")
    return stats


def main():
    parser = argparse.ArgumentParser(description="대전 도매시장 정산 보고서 (기간 + 마지막날 + 전품목)")
    parser.add_argument("start", nargs="?", default=None,
                        help=f"미지정 시 안정화 기준월(오늘-{SETTLE_LAG_DAYS}일의 달) 1일부터 자동")
    parser.add_argument("end", nargs="?", default=None,
                        help=f"미지정 시 4법인 완비 + 오늘-{SETTLE_LAG_DAYS}일 이내 마지막 정산일 자동 탐색")
    parser.add_argument("--out", default=None, help="출력 파일명(미지정 시 settlement_report_시작_to_종료.html)")
    parser.add_argument("--also-daily", action="store_true",
                        help="누계본 생성 후 마지막 정산일 하루치본도 함께 생성")
    parser.add_argument("--force-end", default=None,
                        help="마지막 정산일을 4법인 완비 검증 없이 강제 지정 (YYYY-MM-DD). "
                             "공휴일 등 특정 법인 휴장으로 완비 불가한 날 수동 보고용")
    args = parser.parse_args()

    force = date.fromisoformat(args.force_end) if args.force_end else None

    if args.start is None and force is None:
        # 인자 없음 → 자동 모드: 기준 월을 '오늘-LAG'의 달로 (월 경계 버그 방지)
        start, end = resolve_report_range()
    else:
        start = date.fromisoformat(args.start) if args.start else date(force.year, force.month, 1)
        if force is not None:
            # 강제 모드: find_last_settled_day 우회, 그 날을 마지막 정산일로
            end = force
        elif args.end:
            # 명시 지정 시에는 LAG 컷 없이 그 날을 4법인 완비 기준으로만 탐색 (수동 분석용)
            end = find_last_settled_day(start, date.fromisoformat(args.end))
        else:
            # 공판장 정산 지연 → 오늘-LAG일 이내의 4법인 완비된 마지막 정산일까지만 (미완성 발송 방지)
            end = resolve_auto_end(start)

    print("=" * 60)
    stats = build_report(start, end, args.out, force_last_day=force)

    if args.also_daily and start != end:
        # 마지막 정산일 하루치 단독본 (--out 미적용, 기본 파일명 사용)
        build_report(stats["last_day"], stats["last_day"], None, force_last_day=stats["last_day"])
    print("=" * 60)


if __name__ == "__main__":
    main()
