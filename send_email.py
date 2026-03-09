"""
분석 리포트 이메일 자동 발송
GitHub Actions에서 실행 — Gmail 앱 비밀번호 사용

사용: python send_email.py --date 2026-03-09
"""

import sys
import os
import smtplib
import argparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

REPORT_DIR = Path(__file__).parent / "reports"

WEEKDAYS = ["월", "화", "수", "목", "금", "토", "일"]


def load_report(date: str) -> str | None:
    f = REPORT_DIR / f"report_{date}.md"
    if f.exists():
        return f.read_text(encoding="utf-8")
    return None


def load_compare(date: str) -> str | None:
    """가장 최근 비교 리포트 찾기"""
    for f in sorted(REPORT_DIR.glob(f"compare_*_vs_{date}.md"), reverse=True):
        return f.read_text(encoding="utf-8")
    return None


def md_to_html(md: str) -> str:
    """간단한 마크다운 → HTML 변환"""
    lines = md.split("\n")
    html_lines = []
    in_table = False

    for line in lines:
        if line.startswith("## "):
            html_lines.append(f"<h2>{line[3:]}</h2>")
        elif line.startswith("# "):
            html_lines.append(f"<h1>{line[2:]}</h1>")
        elif line.startswith("**") and line.endswith("**"):
            html_lines.append(f"<p><b>{line[2:-2]}</b></p>")
        elif line.startswith("| ") and "---" not in line:
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if not in_table:
                html_lines.append('<table border="1" cellpadding="6" style="border-collapse:collapse; font-size:13px;">')
                html_lines.append("<tr>" + "".join(f"<th>{c}</th>" for c in cells) + "</tr>")
                in_table = True
            else:
                html_lines.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        elif line.startswith("|") and "---" in line:
            continue
        else:
            if in_table:
                html_lines.append("</table>")
                in_table = False
            if line.startswith("* ") or line.startswith("- "):
                html_lines.append(f"<li>{line[2:]}</li>")
            elif line.strip():
                # Bold 처리
                import re
                line = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', line)
                html_lines.append(f"<p>{line}</p>")

    if in_table:
        html_lines.append("</table>")

    return "\n".join(html_lines)


def send_report(date: str):
    """리포트 이메일 발송"""
    gmail_addr = os.getenv("GMAIL_ADDRESS", "")
    gmail_pw = os.getenv("GMAIL_APP_PASSWORD", "")

    if not gmail_addr or not gmail_pw:
        print("GMAIL_ADDRESS 또는 GMAIL_APP_PASSWORD가 설정되지 않았습니다.")
        print("이메일 발송을 건너뜁니다.")
        return

    report = load_report(date)
    if not report:
        print(f"리포트 없음: {date}")
        return

    compare = load_compare(date)

    # 요일 계산
    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = WEEKDAYS[dt.weekday()]

    # HTML 이메일 구성
    html_body = f"""
<div style="font-family: 'Apple SD Gothic Neo', 'Noto Sans KR', sans-serif; max-width: 700px; margin: 0 auto;">
    <div style="background: #1a1a2e; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
        <h1 style="margin: 0; font-size: 20px;">📊 도매시장 일일분석</h1>
        <p style="margin: 5px 0 0; color: #aaa;">{date} ({weekday}) | 전국 주요 12개 시장</p>
    </div>

    <div style="padding: 20px; background: #f9f9f9; border: 1px solid #ddd;">
        {md_to_html(report)}
    </div>
"""

    if compare:
        html_body += f"""
    <div style="padding: 20px; background: #fff; border: 1px solid #ddd; margin-top: 10px;">
        <h2 style="color: #333;">📈 전일 대비 가격 변동</h2>
        {md_to_html(compare)}
    </div>
"""

    html_body += """
    <div style="padding: 15px; background: #eee; border-radius: 0 0 8px 8px; font-size: 12px; color: #666;">
        data.go.kr 정산 데이터 + Gemini AI 분석 | 자동 생성 by 송봇 (삽질코딩)<br>
        <a href="https://github.com/SongT-50/daily-wholesale-analysis">GitHub</a>
    </div>
</div>
"""

    # 이메일 발송
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[도매시장] {date}({weekday}) 일일분석"
    msg["From"] = gmail_addr
    msg["To"] = gmail_addr
    msg.attach(MIMEText(report, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_addr, gmail_pw)
            server.send_message(msg)
        print(f"이메일 발송 완료: {gmail_addr}")
    except Exception as e:
        print(f"이메일 발송 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description="분석 리포트 이메일 발송")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    send_report(args.date)


if __name__ == "__main__":
    main()
