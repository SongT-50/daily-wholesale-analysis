"""대전 도매시장 정산 보고서 2종을 HTML 첨부로 이메일 발송.

  ① 이번 달 누계본 (5/1 ~ 4법인 정산 완료된 마지막 날)
  ② 마지막 정산일 하루치 단독본

본문에는 요약(물량/금액/품목/검증경고)을 싣고, 두 HTML을 첨부파일로 보낸다.
검증 경고가 있어도 메일은 발송하되 본문·첨부 상단에 경고를 명시한다 (태은이 결재: 경고 달아서 발송).

사용:
  python send_settlement_email.py                # 어제 기준 월
  python send_settlement_email.py --month 2026-05
GitHub Actions: AUCTION_ARCHIVE_DIR=data, SETTLEMENT_OUT_DIR=<workspace> 로 호출.
환경변수 GMAIL_ADDRESS / GMAIL_APP_PASSWORD 필요 (없으면 생성만 하고 발송 skip).
"""
import os, sys, smtplib, argparse, calendar
from pathlib import Path
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

sys.stdout.reconfigure(encoding="utf-8")

from settlement_report import build_report, find_last_settled_day

WEEKDAYS = "월화수목금토일"


def fmt_ton(kg): return f"{kg / 1000:,.1f}"
def fmt_manwon(won): return f"{won / 10000:,.0f}"


def _attach_html(msg, path: Path):
    with open(path, "rb") as fh:
        part = MIMEApplication(fh.read(), _subtype="html")
    part.add_header("Content-Disposition", "attachment", filename=path.name)
    msg.attach(part)


def main():
    parser = argparse.ArgumentParser(description="정산 보고서 2종 HTML 첨부 메일 발송")
    parser.add_argument("--month", default=None, help="YYYY-MM (미지정 시 어제 기준 월)")
    args = parser.parse_args()

    if args.month:
        y, m = (int(x) for x in args.month.split("-"))
    else:
        yest = date.today() - timedelta(days=1)
        y, m = yest.year, yest.month

    start = date(y, m, 1)
    last_dom = calendar.monthrange(y, m)[1]
    end = find_last_settled_day(start, date(y, m, last_dom))

    print("=" * 60)
    print(f"정산 보고서 2종 생성: {y}-{m:02d}")
    cum = build_report(start, end)                          # 누계본
    daily = build_report(cum["last_day"], cum["last_day"])  # 하루치본
    print("=" * 60)

    gmail_addr = os.getenv("GMAIL_ADDRESS", "")
    gmail_pw = os.getenv("GMAIL_APP_PASSWORD", "")
    if not gmail_addr or not gmail_pw:
        print("GMAIL_ADDRESS/GMAIL_APP_PASSWORD 미설정 — 보고서는 생성됐고 메일 발송만 건너뜁니다.")
        return

    last = cum["last_day"]
    last_label = f"{last.month}월 {last.day}일({WEEKDAYS[last.weekday()]})"
    all_warn = list(cum["warnings"]) + list(daily["warnings"])

    if all_warn:
        warn_html = ('<p style="color:#b71c1c;font-weight:bold;">⚠️ 데이터 검증 경고 '
                     f'{len(all_warn)}건 — 첨부 보고서 상단에서 상세 확인</p>'
                     '<ul style="color:#b71c1c;">' +
                     "".join(f"<li>{w}</li>" for w in all_warn) + "</ul>")
    else:
        warn_html = '<p style="color:#1b5e20;font-weight:bold;">✅ 데이터 검증 통과 (집계 정합 · 4법인 완비)</p>'

    body = f"""
<div style="font-family:'Apple SD Gothic Neo','Noto Sans KR',sans-serif;max-width:640px;margin:0 auto;">
  <div style="background:#0d47a1;color:#fff;padding:18px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:19px;">📑 대전 도매시장 정산 보고서</h1>
    <p style="margin:6px 0 0;color:#cfe0ff;">{y}년 {m}월 | 마지막 정산일: {last_label}</p>
  </div>
  <div style="padding:18px;background:#f9f9f9;border:1px solid #ddd;">
    {warn_html}
    <table border="1" cellpadding="7" style="border-collapse:collapse;font-size:13px;width:100%;">
      <tr style="background:#e3f2fd;"><th>구분</th><th>물량(톤)</th><th>금액(만원)</th><th>품목</th></tr>
      <tr><td>5월 누계 ({cum['days']}일)</td><td align="right">{fmt_ton(cum['qty_kg'])}</td>
          <td align="right">{fmt_manwon(cum['amount'])}</td><td align="right">{cum['products']}</td></tr>
      <tr><td>{last_label} 당일</td><td align="right">{fmt_ton(daily['qty_kg'])}</td>
          <td align="right">{fmt_manwon(daily['amount'])}</td><td align="right">{daily['products']}</td></tr>
    </table>
    <p style="margin-top:14px;">📎 첨부파일 2개 — 클릭하면 브라우저로 열립니다.<br>
       &nbsp;&nbsp;① 5월 누계본 &nbsp;②  {last_label} 하루치 단독본</p>
  </div>
  <div style="padding:14px;background:#eee;border-radius:0 0 8px 8px;font-size:12px;color:#666;">
    출처: 농산물유통정보(aT) 정산정보 API | 4법인: 대전중앙청과·원협노은·대전청과·농협대전<br>
    공판장 정산 2~3일 지연 → 4법인 모두 정산 완료된 마지막 날 기준 | 자동 생성 by 송봇
  </div>
</div>
"""

    subject_warn = f" ⚠️경고{len(all_warn)}" if all_warn else ""
    msg = MIMEMultipart()
    msg["Subject"] = f"[대전 정산] {m}월 누계 + {last_label} 당일{subject_warn}"
    msg["From"] = gmail_addr
    msg["To"] = gmail_addr
    msg.attach(MIMEText(body, "html", "utf-8"))
    _attach_html(msg, cum["path"])
    _attach_html(msg, daily["path"])

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_addr, gmail_pw)
            server.send_message(msg)
        print(f"정산 보고서 메일 발송 완료: {gmail_addr} (첨부 2개, 경고 {len(all_warn)}건)")
    except Exception as e:
        print(f"메일 발송 실패: {e}")


if __name__ == "__main__":
    main()
