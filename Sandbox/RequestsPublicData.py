import sys
sys.path.append("..")

from Pipeline.Collector import fetch
from Pipeline.Transformer import transform
from Pipeline.Storage import init_db, save

# DB 초기화 (테이블 없으면 생성)
init_db()
print("DB 초기화 완료")

# 강남구 2026년 2월 매매 수집
df_raw = fetch(trade_type="매매", sigungu_code="11650", ym="202602")
print(f"\n[수집] {len(df_raw)}건")
print("수집 컬럼:", df_raw.columns.tolist())

# 한국어 컬럼으로 변환
df = transform(df_raw, trade_type="매매")
print(f"\n[변환] 컬럼: {df.columns.tolist()}")
print(df.head())

# DB에 저장
saved = save(df, trade_type="매매")
print(f"\n[저장] 신규 저장: {saved}건")
