# PREREG 개정2 — OA-13419 지표 재정렬 (surge/drop balanced accuracy)

> 2026-07-24 WHOLESALE-T3 실행. 설계 owner=CS-T1 (DESIGN `code-security/DESIGN_oa13419_metric_realign_2026-07-24.md`, PIPE #6533 owner지정).
> **이 문서 + `backtest_oa13419_v2_2026-07-24.py`를 결과 보기 前 커밋 = 동결.** 커밋 해시를 RESULT에 기록.
> ★골대이동 아님: 원 PREREG c7181e4 line21/22(결과前 15:21 동결)의 지표(surge/drop 이벤트·H0 v2 동일잣대·3요소)로 **복원**. 실행 v1이 일반방향·2요소로 이탈한 것을 되돌림. 임계·지표·게이트 전부 H0 v2 기존값(새로 유리하게 고른 값 0).

## §1 왜 개정2 (v1 이탈의 교정)
- v1(`backtest_oa13419_2026-07-24.py`, 커밋 3db67c7)은 **일반 매일 방향적중률(raw hit-rate, 모든 날 up/down, 2요소)** 을 쟀다.
- 원 PREREG line22 등록 지표 = **surge/drop 이벤트 기준 + H0 v2 동일 잣대(balanced accuracy)**, line21 반입신호 = **3요소(수준·변화·surge더미)**.
- CS #6530 catch(WHOLESALE 코드 line82/94 독립검증 CONFIRMED) → PIPE #6533 종합 보류 → CS DESIGN(개정2). v1 하네스 건전성(expanding·lookahead·대조군)은 CS 사인오프 통과(#6532), **지표만 재정렬**.

## §2 지표 = H0 v2 정확 복제 (앵커 `wholesale-intelligence/data/analysis/_measure_panel_nextday_2026-07-23.py`)
- **PRICE_THRESHOLD PT = 5.0%** (앵커 v4.PRICE_THRESHOLD, 새로 안 정함).
- **이벤트 라벨** (앵커 line31 `lab`): 각 모드 mode∈{surge,drop} 별로 **익일 대전 raw 변화율 pct[t+1]**:
  - surge: `pct[t+1] > +5.0` → 1, else 0
  - drop: `pct[t+1] < −5.0` → 1, else 0
  - pct = 전 거래일 대비 %(갭 ≤ MAX_GAP=4). **raw 가격 기준**(탈계절 아님 — 이벤트는 실제 관측되는 가격 급등락).
- **지표 = balanced accuracy** = 0.5·(TPR + TNR), TPR=이벤트 재현율, TNR=비이벤트 재현율. raw hit-rate 아님(클래스 불균형 보정 = v1과의 핵심 차이).
- **게이트 (PRIMARY)** = `mean_products[ bacc(test) − bacc(AR) ] ≥ 5.0pp`, **surge·drop 각각**. 둘 다 충족해야 H1(반입 예측력 인정).
- 품목별 산출 후 품목 평균(앵커 agg).
- ⚠️ drop 평균 단독 인용 금지(H0 v2 교훈: 소수품목 견인). 품목별 분해 동반.

## §3 모델 (DESIGN §2)
- **Baseline(AR)** = 탈계절 대전가격 특징으로 이벤트 예측 (v1 구조 유지 = CS "이미 건전"):
  - 특징: `zp[t]`(탈계절 대전가 z), `zp[t]−zp[t−1]`. (expanding train-only 월climatology, v1 그대로.)
- **Test = AR + 가락 경매전 반입(OA-13419) 3요소 (원 line21 복원)**:
  1. `za[t]` — 반입 탈계절 z (수준)
  2. `za[t] − za[t−1]` — 전일대비 변화
  3. **surge더미** — `1 if za[t] > Q_train else 0`. **Q_train = 각 시점 t까지 train 데이터 za의 90분위(상위 10%)** = expanding train-only(lookahead 없음). ★임계분위 = **90분위(상위10%) 고정**(CS 위임분, 결과前 동결, 새로 안 고름).
- **분류기** = v1의 walk-forward expanding logistic (CS 사인오프 PASS분, DESIGN §2 "현 backtest 유지"). ★단 이벤트가 희소(rare)해 raw logit이 전부 음(陰) 예측→bacc=0.5 trivial 위험 → **class-weighted**(표본가중 = 클래스 역빈도, AR·test 양 arm 동일 적용)로 소수클래스 학습. = bacc를 의미있게 만드는 필수 처리(v1 대비 유일 추가, CS 실행판 재검토 대상).
- WARMUP=504 거래일, BLOCK=20, D+4 꼬리 제외(정산 미완비) = v1 동일.

## §4 대조군 (v1 유지, 새 라벨 적용)
- **PC+** = test 특징에 익일 실제 이벤트 라벨(누설) 추가 → bacc 크게 초과해야(하네스 생존·실패가능). ⚠️오라클이라 감도 증명 아님(analyzer 지적, 중강도 합성신호 대조군은 선택).
- **NC** = 반입 za 날짜 무작위 순열 → 게이트 미달해야. 탈계절 後 적용.
- `controls_ok = (PC+ 게이트 초과) AND (NC 게이트 미달)`. real 결과는 controls_ok일 때만 신뢰.

## §5 유의성 검정 (H0 v2 정합, DESIGN §3)
- **품목별 부호검정**: `delta_bacc(품목) > 0` 품목 수 이항검정(H0 v2 "방향 일관 p≈0.015"와 동일 방식), surge·drop 각각.
- **집계 delta_bacc 95% CI**: 품목 delta의 부트스트랩 CI(재표집 2000). "+Xpp가 0과 구별되나 + 5pp 경제게이트 미달/충족" 둘 다 표기.
- ⚠️ **경제게이트(+5pp)와 통계유의성은 다른 문장**(H0 v2 분리표기). "무효과" 단정 금지(v1 일반방향판 +1.26pp도 경계선 p≈0.06–0.11이었음).

## §6 판정 규칙 (결과 보기 前 고정)
- **H1(반입 예측력 인정)** = surge·drop **둘 다** delta_bacc ≥ +5.0pp AND controls_ok.
- **H0 유지(FAIL)** = 어느 한 모드라도 +5pp 미달, 또는 controls_ok 실패.
- 결과 어느 방향이든 이 규칙 고정(anti-fishing). **측정 前 청구·판매 금지.**

## §7 실행·검증 순서
1. 이 PREREG + `backtest_oa13419_v2_2026-07-24.py` **결과 前 커밋**(해시 기록).
2. 실행 → `RESULT_oa13419_v2_2026-07-24.{json,md}`(코드출력 1:1).
3. CS-T1: 게이트5 + 지표정합(앵커 대비) 실행판 재검토. ★특히 class-weighted 처리 승인 여부.
4. measurement-analyzer(G-E) → CO 적대검증(유리결론이든 아니든) → PIPE 종합 → 태은이 특허 최종.

## §8 정합/근거
- 앵커 H0 v2 = `wholesale-intelligence/data/analysis/_measure_panel_nextday_2026-07-23.py` (PT=5.0·lab line31·bacc line124·agg).
- DESIGN = `code-security/DESIGN_oa13419_metric_realign_2026-07-24.md` (CS owner).
- 원 PREREG = `PREREG_oa13419_nowcast_backtest_2026-07-24.md`(개정1b 1503b04) / v1 결과 = `RESULT_oa13419_backtest_2026-07-24.*`(일반방향, 지표 재검토중).
- verification-principles §사전등록(결과前 커밋)·§양성대조군(실패가능)·data-to-claim Gate1(지표정체)·Gate3(재현성).

*제작 터미널: WHOLESALE-T3 · 2026-07-24*
