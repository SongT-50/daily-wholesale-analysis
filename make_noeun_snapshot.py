# -*- coding: utf-8 -*-
"""노은 2법인(중앙청과·원협노은) 일별 물량·금액 스냅샷 생성 (로컬 실행 전용).

GitHub Actions의 data/에는 최근 몇 달치만 남아(cleanup) 노은 보고서의
'작년 동기 대비'가 빈 집계(0%)로 나온다. 로컬 8년 아카이브에서 일별
법인별 합계만 추출해 data/noeun_prev_snapshot.json 으로 저장하면
build_noeun_report 가 아카이브에 없는 기간을 이 파일로 대체한다.

사용법(로컬): python make_noeun_snapshot.py --start 2025-01-01 --end 2026-03-31
범위가 부족해지면(다음 해) 같은 명령으로 범위만 늘려 재생성.
"""
import os, sys, json, argparse
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import settlement_report as sr

J, W = "25000301", "25000302"
OUT = Path(__file__).parent / "data" / "noeun_prev_snapshot.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    args = ap.parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    snap = {}
    cur = start
    while cur <= end:
        recs, _ = sr.load_day(cur)
        agg = {}
        for r in recs:
            code = r.get('corp_code')
            if code not in (J, W):
                continue
            q = r.get('total_qty', 0) or 0
            a = r.get('total_amount', 0) or 0
            if code not in agg:
                agg[code] = [0.0, 0.0]
            agg[code][0] += q
            agg[code][1] += a
        if agg:
            snap[cur.isoformat()] = agg
        cur = date.fromordinal(cur.toordinal() + 1)

    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(snap, f, ensure_ascii=False, separators=(',', ':'))
    tq = sum(v.get(J, [0, 0])[0] + v.get(W, [0, 0])[0] for v in snap.values())
    ta = sum(v.get(J, [0, 0])[1] + v.get(W, [0, 0])[1] for v in snap.values())
    print(f"✅ 스냅샷 {len(snap)}일 ({args.start}~{args.end}) → {OUT}")
    print(f"   노은 2법인 합계: {tq:,.0f}kg / {ta:,.0f}원")


if __name__ == '__main__':
    main()
