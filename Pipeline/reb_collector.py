"""
한국부동산원 R-ONE 주간 아파트 가격동향 수집기

수집 통계:
  - (주) 매매가격지수  : T244183132827305
  - (주) 전세가격지수  : T247713133046872
  - (주) 매매수급동향  : T248163133074619
  - (주) 전세수급동향  : T245423133086632
"""
import os
import sys
import time
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv("R_ONE_API_KEY")
URL = "http://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"

TARGETS = {
    "매매가격지수": "T244183132827305",
    "전세가격지수": "T247713133046872",
    "매매수급동향": "T248163133074619",
    "전세수급동향": "T245423133086632",
}

# 수집 대상 지역 (최상위 + 수도권 광역)
TARGET_REGIONS = {
    "전국", "수도권", "지방권",
    "서울", "경기", "인천",
    "서울>강남지역", "서울>강북지역",
}


def _fetch_all_pages(statbl_id: str) -> pd.DataFrame:
    """전체 페이지네이션으로 통계 전체 수집 (재시도 + 딜레이 포함)"""
    all_rows = []
    page = 1
    page_size = 1000
    max_retries = 5
    delay = 1.5  # 서버 부하 방지용 (초)

    while True:
        params = {
            "KEY":         KEY,
            "TYPE":        "xml",
            "STATBL_ID":   statbl_id,
            "DTACYCLE_CD": "WK",
            "pIndex":      page,
            "pSize":       page_size,
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(URL, params=params, timeout=60)
                break
            except (requests.ConnectionError, requests.Timeout):
                if attempt == max_retries - 1:
                    raise
                wait = (attempt + 1) * 10
                print(f"    [재시도 {attempt+1}/{max_retries}] {wait}초 후 재요청...")
                time.sleep(wait)

        root = ET.fromstring(response.content)

        rows = root.findall(".//row")
        if not rows:
            break

        all_rows.extend([{c.tag: c.text for c in row} for row in rows])
        total = int(root.findtext(".//list_total_count") or 0)

        if len(all_rows) >= total:
            break
        page += 1
        time.sleep(delay)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df = df.rename(columns={
        "WRTTIME_DESC":     "날짜",
        "WRTTIME_IDTFR_ID": "주차코드",
        "CLS_NM":           "지역",
        "CLS_FULLNM":       "지역경로",
        "ITM_NM":           "지표",
        "DTA_VAL":          "값",
        "STATBL_ID":        "통계ID",
    })

    keep = ["통계ID", "날짜", "주차코드", "지역", "지역경로", "지표", "값"]
    df = df[[c for c in keep if c in df.columns]]
    df["날짜"] = pd.to_datetime(df["날짜"])
    df["값"] = pd.to_numeric(df["값"], errors="coerce")

    # 대상 지역만 필터링
    df = df[df["지역경로"].isin(TARGET_REGIONS)]

    return df.reset_index(drop=True)


def fetch_weekly_index(since_date: str | None = None) -> pd.DataFrame:
    """
    주간 아파트 가격동향 수집

    since_date: "2026-01-01" 형식. 지정 시 해당 날짜 이후 데이터만 반환.
                None이면 전체 이력 반환 (최초 수집 시).
    """
    frames = []

    for stat_name, statbl_id in TARGETS.items():
        print(f"  [{stat_name}] 수집 중...")
        df = _fetch_all_pages(statbl_id)

        if df.empty:
            print(f"  [{stat_name}] 데이터 없음")
            continue

        df["통계명"] = stat_name  # API 응답에 통계명 없으므로 직접 추가

        if since_date:
            df = df[df["날짜"] >= pd.Timestamp(since_date)]

        print(f"  [{stat_name}] {len(df)}건")
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
