# 소실 보정 (Missing-Data Corrections) — 회사 정산 자료 세컨드 메모리

> 목적: aT(data.go.kr) 원천에서 **소실된 날의 실제값**을 회사 정산 시스템 자료로 보정한다.
> 이 폴더 = 회사 자료의 **표준 저장소(세컨드 메모리)**. MEMORY.md 볼륨을 늘리지 않고 git으로 영구 박제·재사용.

## 왜 필요한가
- aT 정산 API는 특정 날의 특정 법인 데이터가 **원천에서 통째로 빠지는 소실**(under-count, 재수집 불가)이 있다.
- 반대로 aT 재집계가 특정 월 특정 법인을 **실제보다 과대 집계**(over-count)하기도 한다 — 원협(공판장) 계통출하가 경매에 부풀려 잡히는 등.
- 우리(대전중앙청과)는 본업이라 **회사 자체 정산 시스템·공식 현황판**에 실제값이 있다 → 이게 유일한 authoritative 소스.
- 실증: (소실) 2025 상반기 중앙 4일(1/3·1/4·1/7·1/9)=27.27억, 2026 2/24=5.08억 / (과대) 원협 2026 4·5월 aT 189.9·150.9억 vs 공식 126.2·126.8억. 회사 월계표·공식 현황판과 삼중 검증 일치.

## 두 종류의 보정 (모두 "공식/회사값이 aT를 이긴다")
| 종류 | 방향 | JSON 키 | 사용처 |
|------|------|---------|--------|
| **소실 보정** | aT가 실제보다 **적음**(under) → 실제값 더함 | `missing_days` | 일별·누계 정산, 경매사별 상반기 |
| **공식 override** | aT가 실제보다 **많음**(over) → 공식값으로 교체 | `official_overrides` | ★**노은 월별 집계·그래프** |

### ★ 노은 월별 = 공식 override 우선 (태은이 2026-07-06 지시)
- 태은이 verbatim 취지: "(4·5월 과거 발송분) 보정할 필요 없고, 단지 잊지 말고 다음에 7월 또는 앞으로 할 때도 **공식 자료로 보정한 것으로 진행**."
- **규칙**: 노은 월별(중앙 vs 원협) 집계·그래프를 만들 땐 **가장 먼저 `official_overrides`를 읽어** 해당 월·법인이 있으면 aT 재집계값 대신 **공식값**을 쓴다. (large-data-reading Gate 0 "내부 박제본 먼저"와 동형.)
- 현재 등록: 원협 2026-04·05만 override(중앙청과·나머지 월은 aT=공식 일치라 그대로).
- **과거 재발송 X**: 이미 나간 4·5월 노은/정산 보고서는 손대지 않음(태은이 결정).

## 파일
| 파일 | 내용 |
|------|------|
| `source/` | 회사가 준 원본 엑셀 그대로 보관 (월계표·데이터누락자료 등) |
| `missing_corrections.json` | 파싱된 소실 실제값(날짜별 금액·물량) + 월계표 합계 |
| `apply_missing_corrections.py` | 회사 소실 엑셀 → 품목→경매사 배분 스크립트 |

## SOP — 태은이가 새 회사 정산 자료를 줄 때
1. **원본 보관**: 받은 엑셀을 `corrections/source/`에 원본 그대로 복사.
2. **파싱·박제**: 소실일 실제값(금액·물량)을 `missing_corrections.json`의 `missing_days`에 추가. 월계표면 `ledger_halfyear` 갱신.
3. **경매사 배분(필요 시)**: `python corrections/apply_missing_corrections.py <엑셀경로>` → 품목→경매사 매핑(1/6 대조 학습, 품목명 prefix 매칭). aT 실측 + 소실 = 실제값.
4. **총액 검증**: 보정 후 합계 = `ledger_halfyear`(월계표)와 일치하는지 확인.
5. **커밋**: 이 폴더 변경을 커밋(회사 자료라 대외비 — 우리 private repo만).

## 품목→경매사 매핑 주의
- 회사 품목명(`감`·`파`·`브로코`…)이 aT product와 표기가 달라 ~69%만 자동 매칭, prefix 매칭으로 ~96%.
- 매핑 안 되는 특수품목(허브·두부·묵류 등)은 `소실 미배분`으로 별도 표기(총액엔 포함).
- 매핑 개선하려면 `apply_missing_corrections.py`의 `MANUAL` 사전에 품목 추가.

## 반영된 산출물
- `presentations/auctioneer-halfyear-2025vs2026/print.html` (경매사별 상반기, 소실 실제값 배분 반영)
- `presentations/noeun-monthly-official-vs-ours-2026-07-06/index.html` (노은 월별, 공식 override 반영 — 원협 4·5월 공식값)

## 관련
- 월계표·소실 검증 경위: WHOLESALE-T3 WIP 2026-07-06.
- 정합: `.claude/rules/safe-pruning-preservation.md`(second memory 보존), `data-to-claim-verification.md`(재현성 검증).
