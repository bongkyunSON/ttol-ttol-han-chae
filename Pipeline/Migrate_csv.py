"""
기존 CSV 데이터 → DuckDB 마이그레이션

실행:
  python -m Pipeline.migrate_csv
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from Pipeline.Storage import init_db, get_connection, DB_PATH

DATA_DIR = Path(__file__).parent.parent / "Data"

TRADE_CSV   = DATA_DIR / "Trade_Data.csv"
JEONSE_CSV  = DATA_DIR / "Jeonse_Data.csv"
RENT_CSV    = DATA_DIR / "Rent_Data.csv"

CHUNK_SIZE = 50_000  # 한 번에 읽을 행 수 (메모리 절약)


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼명에서 특수문자/공백 제거 → DB 컬럼명과 통일"""
    df.columns = (
        df.columns
        .str.replace(r"[㎡()（）]", "", regex=True)
        .str.replace("만원", "", regex=False)
        .str.replace(" ", "")
    )
    return df


def migrate_trades() -> int:
    """Trade_Data.csv → trades 테이블"""
    print(f"\n[매매] {TRADE_CSV.name} 마이그레이션 시작...")

    total = 0
    con = get_connection()

    for chunk in pd.read_csv(TRADE_CSV, chunksize=CHUNK_SIZE, dtype=str):
        chunk = _clean_columns(chunk)

        # CSV 전용 컬럼 처리
        chunk = chunk.rename(columns={"시군구": "시군구코드"})  # 전체 이름을 코드 컬럼에 저장
        chunk = chunk.drop(columns=["NO", "동"], errors="ignore")

        before = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        con.register("_chunk", chunk)
        con.execute("INSERT INTO trades BY NAME SELECT * FROM _chunk")
        after = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

        saved = after - before
        total += saved
        print(f"  → {saved}건 저장 (누적: {total}건)")

    con.close()
    print(f"[매매] 완료: 총 {total}건 저장")
    return total


def migrate_rentals() -> int:
    """Jeonse_Data.csv + Rent_Data.csv → rentals 테이블"""
    total = 0
    con = get_connection()

    for csv_path in [JEONSE_CSV, RENT_CSV]:
        print(f"\n[전월세] {csv_path.name} 마이그레이션 시작...")

        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, dtype=str):
            chunk = _clean_columns(chunk)

            chunk = chunk.rename(columns={
                "시군구":         "시군구코드",
                "갱신요구권사용": "갱신요구권사용",
                "종전계약보증금": "종전계약보증금",
                "종전계약월세":   "종전계약월세",
            })
            chunk = chunk.drop(columns=["NO", "본번", "부번"], errors="ignore")

            before = con.execute("SELECT COUNT(*) FROM rentals").fetchone()[0]
            con.register("_chunk", chunk)
            con.execute("INSERT INTO rentals BY NAME SELECT * FROM _chunk")
            after = con.execute("SELECT COUNT(*) FROM rentals").fetchone()[0]

            saved = after - before
            total += saved
            print(f"  → {saved}건 저장 (누적: {total}건)")

    con.close()
    print(f"[전월세] 완료: 총 {total}건 저장")
    return total


if __name__ == "__main__":
    print("=" * 50)
    print("CSV → DuckDB 마이그레이션 시작")
    print("=" * 50)

    init_db()

    trade_total   = migrate_trades()
    rentals_total = migrate_rentals()

    print("\n" + "=" * 50)
    print(f"마이그레이션 완료")
    print(f"  매매    : {trade_total:,}건")
    print(f"  전월세  : {rentals_total:,}건")
    print("=" * 50)
