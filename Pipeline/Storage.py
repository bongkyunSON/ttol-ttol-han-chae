from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "Data" / "real_estate.db"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    """
    DB 테이블이 없으면 생성 (최초 1회 실행)
    trades        : 매매 데이터
    rentals       : 전세 + 월세 데이터 (전월세구분 컬럼으로 구분)
    weekly_index  : 한국부동산원 주간 아파트 가격동향
    """
    con = get_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            시군구코드       VARCHAR,
            법정동          VARCHAR,
            번지            VARCHAR,
            본번            VARCHAR,
            부번            VARCHAR,
            단지명          VARCHAR,
            전용면적        DOUBLE,
            계약년월        VARCHAR,
            계약일          VARCHAR,
            거래금액        VARCHAR,
            층              VARCHAR,
            매수자          VARCHAR,
            매도자          VARCHAR,
            건축년도        VARCHAR,
            도로명          VARCHAR,
            해제사유발생일   VARCHAR,
            거래유형        VARCHAR,
            중개사소재지     VARCHAR,
            등기일자        VARCHAR,
            주택유형        VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS rentals (
            시군구코드           VARCHAR,
            법정동              VARCHAR,
            번지                VARCHAR,
            단지명              VARCHAR,
            전월세구분           VARCHAR,
            전용면적            DOUBLE,
            계약년월            VARCHAR,
            계약일              VARCHAR,
            보증금              VARCHAR,
            월세금              VARCHAR,
            층                  VARCHAR,
            건축년도            VARCHAR,
            도로명              VARCHAR,
            계약기간            VARCHAR,
            계약구분            VARCHAR,
            갱신요구권사용       VARCHAR,
            종전계약보증금       VARCHAR,
            종전계약월세         VARCHAR,
            주택유형            VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS weekly_index (
            통계ID   VARCHAR,
            통계명   VARCHAR,
            날짜     DATE,
            주차코드 VARCHAR,
            지역     VARCHAR,
            지역경로 VARCHAR,
            지표     VARCHAR,
            값       DOUBLE
        )
    """)

    con.close()


def save_weekly_index(df: pd.DataFrame) -> int:
    """
    주간 아파트 가격동향 DataFrame → weekly_index 테이블 저장
    중복 기준: 통계ID + 날짜 + 지역경로 + 지표
    """
    if df.empty:
        return 0

    con = get_connection()
    con.register("_new", df)

    dedup_keys = ["통계ID", "날짜", "지역경로", "지표"]
    where_clause = " AND ".join(f't."{k}" = n."{k}"' for k in dedup_keys)

    insert_sql = f"""
        INSERT INTO weekly_index BY NAME
        SELECT * FROM _new n
        WHERE NOT EXISTS (
            SELECT 1 FROM weekly_index t
            WHERE {where_clause}
        )
    """
    before = con.execute("SELECT COUNT(*) FROM weekly_index").fetchone()[0]
    con.execute(insert_sql)
    after = con.execute("SELECT COUNT(*) FROM weekly_index").fetchone()[0]

    con.close()
    return after - before


def save(df: pd.DataFrame, trade_type: str) -> int:
    """
    DataFrame 을 DB에 저장 (중복 제거 후 신규 건만 INSERT)
    반환값: 실제로 저장된 행 수
    """
    if df.empty:
        return 0

    table = "trades" if trade_type == "매매" else "rentals"

    # 컬럼명에서 괄호/공백 제거 → DB 컬럼명과 맞추기
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace(r"[㎡()（）]", "", regex=True)
        .str.replace("만원", "", regex=False)
        .str.replace(" ", "")
    )

    con = get_connection()

    # DataFrame 을 DuckDB 뷰로 등록 (컬럼 이름 기반 매칭을 위해)
    con.register("_new", df)

    if trade_type == "매매":
        dedup_keys = ["단지명", "계약년월", "계약일", "층", "전용면적", "거래금액"]
    else:
        dedup_keys = ["단지명", "계약년월", "계약일", "층", "전용면적", "보증금", "월세금"]

    where_clause = " AND ".join(f't."{k}" = n."{k}"' for k in dedup_keys)

    # BY NAME: 컬럼 위치가 아닌 이름 기준으로 매핑
    insert_sql = f"""
        INSERT INTO {table} BY NAME
        SELECT * FROM _new n
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} t
            WHERE {where_clause}
        )
    """
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(insert_sql)
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    con.close()
    return after - before
