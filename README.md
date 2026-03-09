# 도매시장 일일 분석 (Daily Wholesale Market Analysis)

전국 공영도매시장 경매 정산 데이터를 매일 자동 수집하고, AI가 분석 리포트를 생성합니다.

## 구조

```
collect.py    — data.go.kr API → 전국 12개 시장 정산 데이터 전량 수집
analyze.py    — Gemini AI → 일일 분석 리포트 (마크다운)
compare.py    — 전일 대비 품목별 가격 변동 분석
run_daily.py  — 통합 파이프라인 (수집 → 분석 → 비교)
```

## 데이터 소스

- **API**: [data.go.kr](https://www.data.go.kr/) — 한국농수산식품유통공사 전국 공영도매시장 실시간 경매정보
- **기준**: 정산일(`trd_clcln_ymd`) = 경매 후 취소 건 제외, 확정 거래
- **규모**: 하루 약 10만 건 (서울가락 4만+)

## 대상 시장 (12개)

서울가락, 서울강서, 인천남촌, 인천삼산, 대전오정, 대전노은, 대구북부, 부산엄궁, 광주각화, 광주서부, 전주, 창원팔용

## 사용법

```bash
# 환경변수 설정
export DATA_GO_KR_API_KEY=your_key
export GEMINI_API_KEY=your_key

# 의존성 설치
pip install -r requirements.txt

# 오늘 데이터 수집 + 분석
python run_daily.py

# 특정 날짜
python run_daily.py --date 2026-03-09

# 개별 실행
python collect.py --date 2026-03-09
python analyze.py --date 2026-03-09
python compare.py --today 2026-03-09 --prev 2026-03-06
```

## GitHub Actions

매일 오전 10시(KST), 월~토 자동 실행. Secrets 필요:
- `DATA_GO_KR_API_KEY`
- `GEMINI_API_KEY`

## 리포트 예시

- [2026-03-09 일일 리포트](reports/report_2026-03-09.md)
- [전일 대비 변동](reports/compare_2026-03-06_vs_2026-03-09.md)

## 만든 사람

삽질코딩 ([@SongT-50](https://github.com/SongT-50)) — 도매시장 + AI
