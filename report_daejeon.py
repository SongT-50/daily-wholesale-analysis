"""대전 지역 전자송품장 출하예약 아침 리포트
매일 아침 실행 → 대전 4개 법인 출하예약 현황 출력

사용: python report_daejeon.py [--date 2026-04-01] [--save] [--telegram]
기본: 오늘 날짜
"""
import sys
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

# 대전 시장 (오정 + 노은)
DAEJEON_MARKETS = {
    "250001": "대전오정",
    "250003": "대전노은",
}

# 관심 법인 (우리 + 경쟁사)
WATCH_CORPS = {
    "대전중앙청과㈜": "★ 우리",
    "대전청과㈜": "경쟁",
    "농협대전(공)": "경쟁",
    "대전원협노은(공)": "경쟁",
}

DATA_DIR = Path(__file__).parent / "data"
REPORT_DIR = Path(__file__).parent.parent / "intelligence"


def load_shipment(date: str) -> dict | None:
    f = DATA_DIR / f"shipment_{date}.json"
    if f.exists():
        with open(f, "r", encoding="utf-8") as fp:
            return json.load(fp)
    return None


def collect_if_needed(date: str) -> dict | None:
    """데이터 없으면 수집 시도"""
    data = load_shipment(date)
    if data:
        # 대전 시장 데이터가 있는지 확인
        has_daejeon = any(m in data.get("markets", {}) for m in DAEJEON_MARKETS)
        if has_daejeon:
            return data

    # 수집 실행
    print(f"[수집] {date} 대전 출하예약 수집 중...")
    from collect_shipment import collect_shipment, DEFAULT_MARKETS
    # 대전만 수집하면 다른 시장 데이터를 덮어쓰니까, 전국 수집
    result = collect_shipment(date, DEFAULT_MARKETS)
    return result


def build_report(data: dict, date: str) -> str:
    """대전 법인별 출하예약 리포트 생성"""
    lines = []
    lines.append(f"# 대전 전자송품장 출하예약 리포트 — {date}")
    lines.append(f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    total_all_qty = 0
    total_all_kg = 0.0

    for mcode, mname in DAEJEON_MARKETS.items():
        market = data.get("markets", {}).get(mcode, {})
        items = market.get("items", [])

        if not items:
            lines.append(f"## {mname} ({mcode}) — 출하예약 없음")
            lines.append("")
            continue

        # 법인별 분리
        corp_data = defaultdict(list)
        for item in items:
            corp_data[item["corp_name"]].append(item)

        for corp_name, corp_items in sorted(corp_data.items()):
            tag = WATCH_CORPS.get(corp_name, "")
            tag_str = f" {tag}" if tag else ""

            # 품목별 집계
            product_agg = defaultdict(lambda: {
                "qty": 0, "kg": 0.0, "grades": set(),
                "trade_types": set(), "trade_methods": set(),
                "statuses": set(),
            })

            for item in corp_items:
                key = f"{item['product']} ({item['variety']})"
                a = product_agg[key]
                qty = item.get("quantity", 0)
                wt = item.get("unit_weight", 0)
                a["qty"] += qty
                a["kg"] += qty * wt
                grade = item.get("grade", "")
                if grade and grade != ".":
                    a["grades"].add(grade)
                tt = item.get("trade_type", "")
                if tt and tt != "-":
                    a["trade_types"].add(tt)
                tm = item.get("trade_method", "")
                if tm:
                    a["trade_methods"].add(tm)
                st = item.get("status", "")
                if st:
                    a["statuses"].add(st)

            total_qty = sum(a["qty"] for a in product_agg.values())
            total_kg = sum(a["kg"] for a in product_agg.values())
            total_all_qty += total_qty
            total_all_kg += total_kg

            lines.append(f"## {corp_name} ({mname}){tag_str}")
            lines.append(f"총 {total_qty:,}건 / {total_kg:,.0f}kg")
            lines.append("")
            lines.append(f"| 품목 | 수량 | 중량 | 등급 | 매매 | 거래 | 상태 |")
            lines.append(f"|------|------|------|------|------|------|------|")

            for key, a in sorted(product_agg.items(), key=lambda x: -x[1]["kg"]):
                grades = ",".join(sorted(a["grades"])) or "-"
                tts = ",".join(sorted(a["trade_types"])) or "-"
                tms = ",".join(sorted(a["trade_methods"])) or "-"
                sts = ",".join(sorted(a["statuses"])) or "-"
                lines.append(
                    f"| {key} | {a['qty']:,}건 | {a['kg']:,.0f}kg | {grades} | {tts} | {tms} | {sts} |"
                )

            lines.append("")

    lines.append(f"---")
    lines.append(f"**대전 전체**: {total_all_qty:,}건 / {total_all_kg:,.0f}kg")

    return "\n".join(lines)


def send_telegram(text: str):
    """텔레그램으로 리포트 전송"""
    import httpx

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        print("TELEGRAM 환경변수 미설정 — 발송 건너뜀")
        return

    # 텔레그램 4096자 제한
    if len(text) > 4000:
        text = text[:4000] + "\n\n... (잘림)"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    resp = httpx.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }, timeout=15)

    if resp.status_code == 200:
        print("텔레그램 발송 완료")
    else:
        # 마크다운 실패 시 일반 텍스트 재시도
        resp2 = httpx.post(url, json={"chat_id": chat_id, "text": text}, timeout=15)
        if resp2.status_code == 200:
            print("텔레그램 발송 완료 (plain text)")
        else:
            print(f"텔레그램 발송 실패: {resp2.status_code}")


def build_telegram_message(report: str, date: str) -> str:
    """리포트를 텔레그램용으로 변환"""
    dt = datetime.strptime(date, "%Y-%m-%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    wd = weekdays[dt.weekday()]

    msg = f"📦 대전 출하예약 {date}({wd})\n\n"
    # 마크다운 테이블은 텔레그램에서 안 되니까 텍스트로 변환
    for line in report.split("\n"):
        if line.startswith("## "):
            corp = line.replace("## ", "").strip()
            msg += f"*{corp}*\n"
        elif line.startswith("총 "):
            msg += f"  {line}\n"
        elif line.startswith("| ") and "---" not in line and "품목" not in line:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) >= 4:
                msg += f"  • {cells[0]}: {cells[1]} / {cells[2]} ({cells[3]}, {cells[4]})\n"
        elif line.startswith("**대전 전체**"):
            msg += f"\n{line.replace('**', '')}\n"

    msg += "\n🤖 송봇 자동 리포트"
    return msg


def main():
    parser = argparse.ArgumentParser(description="대전 전자송품장 아침 리포트")
    today = datetime.now().strftime("%Y-%m-%d")
    parser.add_argument("--date", default=today, help="출하예정일 (기본: 오늘)")
    parser.add_argument("--save", action="store_true", help="intelligence/에 저장")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 발송")
    args = parser.parse_args()

    data = collect_if_needed(args.date)
    if not data:
        print(f"{args.date} 데이터 수집 실패")
        sys.exit(1)

    report = build_report(data, args.date)
    print(report)

    if args.save:
        REPORT_DIR.mkdir(exist_ok=True)
        out = REPORT_DIR / f"daejeon-shipment-{args.date}.md"
        with open(out, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\n저장: {out}")

    if args.telegram:
        msg = build_telegram_message(report, args.date)
        send_telegram(msg)


if __name__ == "__main__":
    main()
