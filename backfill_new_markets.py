"""신규 20개 시장 과거 데이터 보충 수집 (backfill②)
기존 파일(12개 시장)에 신규 20개 시장 데이터를 병합.
아카이브(wholesale-data/)가 정본. data/에는 당월만 유지.

사용:
  python backfill_new_markets.py                    # 전체 기존 파일 대상
  python backfill_new_markets.py --max 300          # 최대 300일
  python backfill_new_markets.py --dry-run          # 수집 안 하고 대상만 출력
"""
import sys
import json
import argparse
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from collect import collect, DEFAULT_MARKETS, VALID_CORPS, RateLimitError

DATA_DIR = Path(__file__).parent / "data"
ARCHIVE_DIR = Path(__file__).parent.parent.parent / "wholesale-data"

# 기존 12개 시장 (원래 수집하던 것)
OLD_MARKETS = {
    "110001", "110008", "230001", "230003", "250001", "250003",
    "220001", "210001", "240001", "240004", "350101", "380101",
}
# 32개 시장 기준 market_count 판별
FULL_MARKET_COUNT = 32

# 신규 17개 시장만
NEW_MARKETS = {k: v for k, v in DEFAULT_MARKETS.items() if k not in OLD_MARKETS}


def _check_needs_supplement(filepath: Path) -> bool:
    """파일의 market_count가 FULL_MARKET_COUNT 미만이고 보충 미완료면 True"""
    try:
        with open(filepath, "r", encoding="utf-8") as fp:
            # 앞부분에서 market_count 확인, 플래그는 파일 어디든 있을 수 있으므로 별도 체크
            head = fp.read(2048)
        import re
        m = re.search(r'"market_count":\s*(\d+)', head)
        if m and int(m.group(1)) >= FULL_MARKET_COUNT:
            return False
        # supplement_complete 플래그는 파일 끝에 있을 수 있음
        if '"supplement_complete": true' in head or '"supplement_complete":true' in head:
            return False
        # 앞부분에 없으면 끝부분도 확인
        file_size = filepath.stat().st_size
        if file_size > 2048:
            with open(filepath, "r", encoding="utf-8") as fp:
                fp.seek(max(0, file_size - 512))
                tail = fp.read()
            if '"supplement_complete": true' in tail or '"supplement_complete":true' in tail:
                return False
    except Exception:
        pass
    return True


def get_target_files() -> list[tuple[str, Path]]:
    """기존 데이터 파일 중 신규 시장이 빠진 것들 (날짜 내림차순)

    아카이브(wholesale-data/)를 정본으로 스캔.
    data/는 당월 작업용이므로 backfill 대상에서 제외.
    """
    date_to_file: dict[str, Path] = {}

    # 아카이브 스캔 (정본)
    if ARCHIVE_DIR.exists():
        for month_dir in sorted(ARCHIVE_DIR.iterdir()):
            if not month_dir.is_dir():
                continue
            for f in month_dir.glob("auction_*.json"):
                date_str = f.stem.replace("auction_", "")
                if len(date_str) == 10:
                    date_to_file[date_str] = f

    # market_count < FULL_MARKET_COUNT인 것만 필터
    targets = []
    for date_str, filepath in date_to_file.items():
        if _check_needs_supplement(filepath):
            targets.append((date_str, filepath))

    # 날짜 내림차순 정렬
    targets.sort(key=lambda x: x[0], reverse=True)
    return targets


def _mark_supplement_complete(filepath: Path):
    """파일에 supplement_complete 플래그 추가 (재시도 방지)"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["supplement_complete"] = True
        data["supplement_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        # 아카이브에도 반영
        date_str = data.get("date", "")
        if date_str:
            archive_file = ARCHIVE_DIR / date_str[:7] / filepath.name
            if archive_file.exists() and archive_file != filepath:
                with open(archive_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠️ 플래그 기록 실패: {e}")


def merge_data(existing_file: Path, new_result: dict) -> int:
    """기존 파일에 신규 시장 데이터 병합. 추가된 건수 반환."""
    with open(existing_file, "r", encoding="utf-8") as f:
        existing = json.load(f)

    added_count = 0
    existing_markets = existing.get("markets", {})
    new_markets = new_result.get("markets", {})

    for code, market_data in new_markets.items():
        items = market_data.get("items", [])
        if not items:
            continue
        if code not in existing_markets:
            existing_markets[code] = market_data
            added_count += len(items)
        else:
            # 기존에 있으면 건수 비교해서 많으면 교체
            old_count = existing_markets[code].get("collected", 0)
            new_count = market_data.get("collected", 0)
            if new_count > old_count:
                existing_markets[code] = market_data
                added_count += new_count - old_count

    # 메타데이터 업데이트
    existing["markets"] = existing_markets
    existing["market_count"] = len(existing_markets)
    total_collected = sum(
        m.get("collected", 0) for m in existing_markets.values()
    )
    existing["total_collected"] = total_collected

    # 아카이브에 저장 (정본, 항상 확실하게)
    date_str = existing.get("date", "")
    filename = existing_file.name

    if date_str:
        month_dir = ARCHIVE_DIR / date_str[:7]
        month_dir.mkdir(parents=True, exist_ok=True)
        archive_file = month_dir / filename
        with open(archive_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        # existing_file이 아카이브가 아닌 경우에도 아카이브가 정본
        if existing_file != archive_file:
            with open(existing_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
    else:
        # date_str 없으면 원본 파일에만 저장
        with open(existing_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)

    # data/에는 복사하지 않음 — data/는 당월 작업용, 아카이브가 정본

    return added_count


def main():
    parser = argparse.ArgumentParser(description="신규 17개 시장 과거 데이터 보충")
    parser.add_argument("--max", type=int, default=300,
                        help="1회 최대 날짜 수 (기본: 300)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="날짜 간 대기 초 (기본: 1.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="수집 안 하고 대상만 출력")
    args = parser.parse_args()

    print(f"{'=' * 60}")
    print(f"  신규 17개 시장 과거 데이터 보충 수집")
    print(f"  대상 시장: {len(NEW_MARKETS)}개 (신규 추가분)")
    for code, name in sorted(NEW_MARKETS.items()):
        print(f"    {code}: {name}")
    print(f"{'=' * 60}")

    targets = get_target_files()
    print(f"\n보충 대상: {len(targets)}개 파일")

    if len(targets) > args.max:
        targets = targets[:args.max]
        print(f"1회 최대 {args.max}일 제한 적용")

    if not targets:
        print("\n보충할 파일이 없습니다. ✓")
        return

    if args.dry_run:
        print(f"\n[Dry Run] 대상 날짜 (아카이브 기준):")
        for i, (date, f) in enumerate(targets[:20], 1):
            print(f"  {i}. {date}")
        if len(targets) > 20:
            print(f"  ... 외 {len(targets) - 20}개")
        print(f"\n총 {len(targets)}일 (아카이브 정본)")
        est = len(targets) * (args.delay + 8) / 60
        print(f"예상 API 호출: {len(targets)}건 | ~{est:.0f}분")
        return

    # 수집 실행
    success_count = 0
    total_added = 0
    error_count = 0
    t0 = time.time()

    for i, (date, filepath) in enumerate(targets, 1):
        print(f"\n[{i}/{len(targets)}] {date}")

        try:
            # 아카이브 원본 백업 (collect가 파일을 덮어쓰므로)
            with open(filepath, "r", encoding="utf-8") as f:
                original_data = json.load(f)

            # collect() 호출 — 예외 시 아카이브 원본 복원 보장
            result = None
            try:
                result = collect(date, NEW_MARKETS)
            except Exception:
                # 예외 시 collect가 아카이브를 덮어썼을 수 있으므로 원본 복원
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(original_data, f, ensure_ascii=False, indent=2)
                raise

            # 정상 경로: collect가 덮어쓴 아카이브를 원본으로 복원 후 병합
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(original_data, f, ensure_ascii=False, indent=2)

            # collect가 data/에 생성한 파일 삭제 (당월 아니면 불필요)
            data_file = DATA_DIR / f"auction_{date}.json"
            current_month = datetime.now().strftime("%Y-%m")
            if data_file.exists() and not date.startswith(current_month):
                data_file.unlink()

            new_count = result.get("total_collected", 0) if result else 0

            if new_count > 0:
                added = merge_data(filepath, result)
                total_added += added
                success_count += 1
                if added > 0:
                    _mark_supplement_complete(filepath)
                    print(f"  → +{added}건 병합 완료")
                else:
                    # 데이터 있지만 이미 전부 병합됨 → 보충 완료 표시
                    _mark_supplement_complete(filepath)
                    success_count += 0  # 이미 카운트됨
                    print(f"  → 이미 병합 완료, 보충 완료 표시")
            else:
                # API에 데이터 자체가 없음 → 보충 완료 표시
                _mark_supplement_complete(filepath)
                print(f"  → 신규 시장 데이터 없음, 보충 완료 표시")

        except RateLimitError:
            print(f"\n⚠️  API 일일 한도 초과 — 자동 중단")
            print(f"  내일 같은 명령으로 재실행하면 이어서 수집됩니다.")
            break
        except Exception as e:
            error_count += 1
            print(f"  → ERROR: {e}")

        elapsed = time.time() - t0
        avg = elapsed / i
        eta = (len(targets) - i) * avg
        print(f"  [{i}/{len(targets)}] 성공:{success_count} 에러:{error_count} "
              f"추가:{total_added:,}건 | ETA ~{eta/60:.0f}분")

        if i < len(targets):
            time.sleep(args.delay)

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  보충 수집 완료")
    print(f"{'=' * 60}")
    print(f"  소요: {elapsed/60:.1f}분")
    print(f"  성공: {success_count}일")
    print(f"  추가: {total_added:,}건")
    print(f"  에러: {error_count}일")


if __name__ == "__main__":
    main()
