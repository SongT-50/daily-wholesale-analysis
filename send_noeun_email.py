# -*- coding: utf-8 -*-
"""노은도매시장 거래현황 보고서 자동 메일 발송 (본인함).

build_noeun_report.generate_html 재사용 + SMTP (send_settlement_email.py와 동일 패턴).
GMAIL_ADDRESS / GMAIL_APP_PASSWORD 필요 (없으면 생성만).

사용법:
  python send_noeun_email.py                   # 자동(6월 누계, 마지막 정산일까지)
  python send_noeun_email.py --end 2026-06-27  # 종료일 강제
"""
import os, sys, smtplib, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv('C:/Users/samsung/2026/02/monet/.env')
except Exception:
    pass

import settlement_report as sr
import build_noeun_report as bn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--end')
    args = ap.parse_args()
    end = date.fromisoformat(args.end) if args.end else sr.resolve_report_range()[1]

    html, meta = bn.generate_html(end)
    # 저장 경로: NOEUN_OUT_DIR(GitHub Actions 등) 지정 시 그곳, 아니면 로컬 presentations 박제
    out_base = os.getenv('NOEUN_OUT_DIR')
    if out_base:
        os.makedirs(out_base, exist_ok=True)
        path = os.path.join(out_base, f'noeun_{end.isoformat()}.html')
    else:
        outdir = os.path.join(bn.MONET, 'presentations', f'noeun-market-report-{end.isoformat()}')
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, 'index.html')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)

    vol, amt = meta['vol'], meta['amt']
    ja, wa = meta['ja'], meta['wa']
    amt25, vol25 = meta['amt25'], meta['vol25']
    start, days = meta['start'], meta['days']
    trend = '▲ 우세 확대' if amt >= amt25 else '▽ 축소'
    gap = (ja - wa) / 1e8
    wd = '월화수목금토일'[end.weekday()]

    gmail_addr = os.getenv('GMAIL_ADDRESS', '')
    gmail_pw = os.getenv('GMAIL_APP_PASSWORD', '')
    if not gmail_addr or not gmail_pw:
        print(f"보고서 생성됨: {path}")
        print("GMAIL_ADDRESS/GMAIL_APP_PASSWORD 미설정 — 메일 발송만 건너뜁니다.")
        return

    body = f"""
<div style="font-family:'Apple SD Gothic Neo','Noto Sans KR',sans-serif;max-width:640px;margin:0 auto;">
  <div style="background:#0d47a1;color:#fff;padding:18px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:19px;">🥊 노은도매시장 거래현황 — 중앙청과 vs 원협노은</h1>
    <p style="margin:6px 0 0;color:#cfe0ff;">{end.year}년 {end.month}월 누계 ({start.month}/{start.day} ~ {end.month}/{end.day}, {days}영업일)</p>
  </div>
  <div style="padding:18px;background:#f9f9f9;border:1px solid #ddd;">
    <table border="1" cellpadding="7" style="border-collapse:collapse;font-size:13px;width:100%;">
      <tr style="background:#e3f2fd;"><th>구분</th><th>중앙청과 (우리)</th><th>원협노은</th></tr>
      <tr><td>금액 점유율</td><td align="right"><b style="color:#0d47a1;">{amt:.1f}%</b></td><td align="right">{100-amt:.1f}%</td></tr>
      <tr><td>물량 점유율</td><td align="right"><b style="color:#0d47a1;">{vol:.1f}%</b></td><td align="right">{100-vol:.1f}%</td></tr>
      <tr><td>금액 (누계)</td><td align="right">{ja/1e8:.1f}억</td><td align="right">{wa/1e8:.1f}억</td></tr>
    </table>
    <p style="margin-top:12px;font-size:13px;">
      금액 격차: 중앙 {'＋' if gap>=0 else '−'}{abs(gap):.1f}억<br>
      작년 동기 대비: 금액점유 {amt25:.1f}% → <b>{amt:.1f}%</b> {trend}</p>
    <p style="margin-top:10px;">📎 첨부: <b>경매사별 상세 + 작년 대비</b> 한 장 보고서 (인쇄용)</p>
  </div>
  <div style="padding:14px;background:#eee;border-radius:0 0 8px 8px;font-size:12px;color:#666;">
    출처: 도매시장통합 정산자료 (노은시장 중앙청과·원협노은 2법인)<br>
    공판장 정산 2~3일 지연 → 4법인 모두 정산 완료된 마지막 날 기준 | 자동 생성 by 송봇
  </div>
</div>
"""
    msg = MIMEMultipart()
    msg['Subject'] = f"[노은 거래현황] {end.month}월 누계 + {end.month}/{end.day}({wd}) — 금액점유 중앙 {amt:.1f}%"
    msg['From'] = gmail_addr
    msg['To'] = gmail_addr
    msg.attach(MIMEText(body, 'html', 'utf-8'))
    with open(path, 'rb') as f:
        att = MIMEApplication(f.read(), _subtype='html')
    att.add_header('Content-Disposition', 'attachment',
                   filename=f'노은도매시장_거래현황_{end.year}년{end.month}월누계_{end.isoformat()}.html')
    msg.attach(att)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_addr, gmail_pw)
            server.send_message(msg)
        print(f"노은 보고서 메일 발송 완료: {gmail_addr}")
        print(f"  {end.month}월 누계 금액점유 중앙 {amt:.1f}%(작년 {amt25:.1f}%) · 중앙 {ja/1e8:.1f}억 vs 원협 {wa/1e8:.1f}억")
    except Exception as e:
        print(f"메일 발송 실패: {e}")


if __name__ == '__main__':
    main()
