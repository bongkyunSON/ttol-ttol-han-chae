import sys
sys.path.append("..")

import duckdb
con = duckdb.connect("../Data/real_estate.db")

print("=== 테이블별 건수 ===")
print("trades      :", con.execute("SELECT COUNT(*) FROM trades").fetchone()[0], "건")
print("rentals     :", con.execute("SELECT COUNT(*) FROM rentals").fetchone()[0], "건")
print("weekly_index:", con.execute("SELECT COUNT(*) FROM weekly_index").fetchone()[0], "건")

print("\n=== trades 샘플 (강남구 매매) ===")
df = con.execute("""
    SELECT 시군구코드, 단지명, 계약년월, 거래금액
    FROM trades
    WHERE 시군구코드 LIKE '%강남구%'
    ORDER BY 계약년월 DESC
    LIMIT 5
""").df()
print(df.to_string())

print("\n=== weekly_index 샘플 (강남지역 매매가격지수 최근) ===")
df2 = con.execute("""
    SELECT 통계명, 날짜, 지역, 지표, 값
    FROM weekly_index
    WHERE 통계명 = '매매가격지수' AND 지역경로 = '서울>강남지역'
    ORDER BY 날짜 DESC
    LIMIT 5
""").df()
print(df2.to_string())

print("\n=== 데이터 기간 ===")
print("매매   :", con.execute("SELECT MIN(계약년월), MAX(계약년월) FROM trades").fetchone())
print("전월세 :", con.execute("SELECT MIN(계약년월), MAX(계약년월) FROM rentals").fetchone())
print("주간동향:", con.execute("SELECT MIN(날짜), MAX(날짜) FROM weekly_index").fetchone())
