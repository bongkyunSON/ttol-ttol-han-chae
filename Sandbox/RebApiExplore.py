import sys
sys.path.append("..")

from Pipeline.reb_collector import fetch_weekly_index
from Pipeline.Storage import init_db, save_weekly_index

# DB 초기화 (weekly_index 테이블 생성)
init_db()

# 2026년 데이터 수집
df = fetch_weekly_index(since_date="2026-01-01")
print(f"수집: {len(df)}건")

# DB 저장
saved = save_weekly_index(df)
print(f"신규 저장: {saved}건")

# 저장된 데이터 조회
import duckdb
con = duckdb.connect("../Data/real_estate.db")
print("\n=== DB 저장 확인 ===")
result = con.execute("""
    SELECT 통계명, 지역경로, 날짜, 값
    FROM weekly_index
    WHERE 지역경로 = '전국'
    ORDER BY 통계명, 날짜
""").df()
print(result.to_string())
con.close()
