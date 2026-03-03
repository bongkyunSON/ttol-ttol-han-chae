"""
파이프라인 진입점

사용법:
  # 이어서 수집 (DB의 마지막 수집월 다음달부터 저번달까지)
  python -m Pipeline.run

  # 특정 시작월 지정 (최초 수집 or 재수집 시)
  python -m Pipeline.run --start 202001
"""
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from Pipeline.Collector import fetch_all
from Pipeline.Transformer import transform
from Pipeline.Storage import init_db, save, save_weekly_index, get_connection
from Pipeline.State import get_target_ym_list, get_last_ym
from Pipeline.reb_collector import fetch_weekly_index
from Utils.SigunguMapping import get_all_codes

TRADE_TYPES = ["매매", "전월세"]


def run(start_ym: str | None = None) -> None:
    print("=" * 50)
    print("Go-KangNam 부동산 데이터 파이프라인 시작")
    print("=" * 50)

    init_db()

    sigungu_codes = get_all_codes()
    print(f"수집 대상 시군구: {len(sigungu_codes)}개")

    for trade_type in TRADE_TYPES:
        print(f"\n{'─' * 40}")
        print(f"[{trade_type}] 수집 시작")

        try:
            ym_list = get_target_ym_list(trade_type, start_ym=start_ym)
        except ValueError as e:
            # DB가 비어있고 start_ym도 없는 경우
            print(f"[SKIP] {e}")
            continue

        if not ym_list:
            last = get_last_ym(trade_type)
            print(f"[SKIP] 수집할 월 없음 (마지막 수집: {last}, 아직 최신 상태)")
            continue

        print(f"[{trade_type}] 수집 대상 월: {ym_list}")

        df_raw = fetch_all(trade_type, sigungu_codes, ym_list)

        if df_raw.empty:
            print(f"[{trade_type}] 수집된 데이터 없음")
            continue

        df = transform(df_raw, trade_type)
        saved = save(df, trade_type)

        print(f"[{trade_type}] 완료: 수집 {len(df)}건 / 신규 저장 {saved}건")

    # ── 주간 아파트 가격동향 (한국부동산원) ──────────────
    print(f"\n{'─' * 40}")
    print("[주간동향] 수집 시작")

    con = get_connection()
    last_date = con.execute("SELECT MAX(날짜) FROM weekly_index").fetchone()[0]
    con.close()

    since = str(last_date) if last_date else "2012-01-01"
    df_weekly = fetch_weekly_index(since_date=since)

    if df_weekly.empty:
        print("[주간동향] 수집된 데이터 없음")
    else:
        saved_w = save_weekly_index(df_weekly)
        print(f"[주간동향] 완료: 수집 {len(df_weekly)}건 / 신규 저장 {saved_w}건")

    print("\n" + "=" * 50)
    print("파이프라인 완료")
    print("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="수집 시작 년월 (예: 202001). 미지정 시 DB 마지막 수집월 다음달부터.",
    )
    args = parser.parse_args()
    run(start_ym=args.start)
