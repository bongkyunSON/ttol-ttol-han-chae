"""
한국부동산원 주간 아파트 가격동향 전체 이력 로드 (최초 1회)
2012년부터 현재까지 전체 저장
"""
import sys
sys.path.append("..")

from Pipeline.reb_collector import fetch_weekly_index
from Pipeline.Storage import init_db, save_weekly_index

init_db()

print("전체 이력 수집 중... (30~50분 소요)")
df = fetch_weekly_index(since_date=None)  # 전체 이력

if df.empty:
    print("수집된 데이터 없음")
    exit()

print(f"\n수집 완료: {len(df)}건")
print(f"날짜 범위: {df['날짜'].min().date()} ~ {df['날짜'].max().date()}")
print(f"통계명: {df['통계명'].unique().tolist()}")
print(f"지역 수: {df['지역경로'].nunique()}개")

saved = save_weekly_index(df)
print(f"신규 저장: {saved}건")
