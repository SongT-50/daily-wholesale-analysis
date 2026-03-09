"""
분석 리포트 텔레그램 자동 발송
GitHub Actions에서 실행

사용: python send_telegram.py --date 2026-03-09
"""

import sys
import os
import argparse
from datetime import datetime
from pathlib import Path

import httpx

sys.stdout.reconfigure(encoding="utf-8")

REPORT_DIR = Path(__file__).parent / "reports"
WEEKDAYS = ["월", "화", "수", "목", "금", "토", "일"]

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def load_report(date: str) -> str | None:
    f = REPORT_DIR / f"report_{date}.md"
    if f.exists():
        return f.read_text(encoding="utf-8")
    return None


def load_compare(date: str) -> str | None:
    for f in sorted(REPORT_DIR.glob(f"compare_*_vs_{date}.md"), reverse=True):
        return f.read_text(encoding="utf-8")
    return None


def send_telegram(text: str):
    """텔레그램 메시지 전송 (마크다운)"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    # 텔레그램은 4096자 제한
    if len(text) > 4000:
        text = text[:4000] + "\n\n... (전체 리포트는 이메일 확인)"

    resp = httpx.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }, timeout=15)

    if resp.status_code == 200:
        print("텔레그램 발송 완료")
    else:
        # 마크다운 파싱 실패 시 일반 텍스트로 재시도
        resp2 = httpx.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
        }, timeout=15)
        if resp2.status_code == 200:
            print("텔레그램 발송 완료 (plain text)")
        else:
            print(f"텔레그램 발송 실패: {resp2.status_code} {resp2.text}")


def build_message(date: str) -> str:
    """텔레그램용 메시지 구성"""
    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]

    report = load_report(date)
    if not report:
        return f"📊 {date}({weekday}) 리포트 없음"

    compare = load_compare(date)

    msg = f"📊 도매시장 일일분석 {date}({weekday})\n\n"
    msg += report

    if compare:
        # 비교표에서 핵심만 추출 (상승/하락 요약)
        lines = compare.split("\n")
        summary_line = [l for l in lines if l.startswith("**요약**")]
        top_changes = []
        for l in lines:
            if "🔺" in l and ("+4" in l or "+5" in l or "+6" in l or "+7" in l or "+8" in l or "+9" in l or "+1" in l or "+2" in l):
                parts = l.split("|")
                if len(parts) >= 5:
                    name = parts[1].strip()
                    change = parts[3].strip()
                    top_changes.append(f"  {change} {name}")
            elif "🔻" in l and ("-1" in l or "-2" in l or "-3" in l or "-4" in l or "-5" in l):
                parts = l.split("|")
                if len(parts) >= 5:
                    name = parts[1].strip()
                    change = parts[3].strip()
                    top_changes.append(f"  {change} {name}")

        if top_changes:
            msg += "\n📈 주요 변동:\n"
            msg += "\n".join(top_changes[:10])

        if summary_line:
            msg += "\n" + summary_line[0].replace("**", "")

    msg += "\n\n🤖 자동 생성 by 송봇"
    return msg


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 설정되지 않았습니다.")
        print("텔레그램 발송을 건너뜁니다.")
        return

    parser = argparse.ArgumentParser(description="분석 리포트 텔레그램 발송")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    msg = build_message(args.date)
    send_telegram(msg)


if __name__ == "__main__":
    main()
