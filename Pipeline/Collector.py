import os
import time
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
SERVICE_KEY = os.getenv("MOLTI_API_KEY")

TRADE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"


def fetch(trade_type: str, sigungu_code: str, ym: str) -> pd.DataFrame:
    """
    trade_type : "매매" or "전월세"
    sigungu_code: 시군구코드 5자리 (예: "11650")
    ym          : 계약년월 6자리 (예: "202603")
    반환        : 영어 컬럼의 DataFrame (컬럼 rename 은 transformer 에서)
    """
    url = TRADE_URL if trade_type == "매매" else RENT_URL

    params = {
        "serviceKey": SERVICE_KEY,
        "LAWD_CD": sigungu_code,
        "DEAL_YMD": ym,
        "numOfRows": 9999,
        "pageNo": 1,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] {trade_type} {sigungu_code} {ym} 요청 실패: {e}")
        return pd.DataFrame()

    root = ET.fromstring(response.content)

    # API 오류 응답 감지 (정상일 때는 resultMsg 가 없거나 "OK" / "NORMAL SERVICE")
    result_msg = root.findtext(".//resultMsg") or ""
    if result_msg.upper() not in ("", "OK", "NORMAL SERVICE"):
        print(f"[WARN] {trade_type} {sigungu_code} {ym} API 오류: {result_msg}")
        return pd.DataFrame()

    rows = []
    for item in root.findall(".//item"):
        row = {child.tag: child.text for child in item}
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def fetch_all(trade_type: str, sigungu_codes: list[str], ym_list: list[str], delay: float = 0.5) -> pd.DataFrame:
    """
    여러 시군구코드 × 여러 월을 순회하며 수집 후 하나의 DataFrame 으로 반환
    delay: API 호출 간격 (초) - 서버 부하 방지
    """
    frames = []

    total = len(sigungu_codes) * len(ym_list)
    done = 0

    for ym in ym_list:
        for code in sigungu_codes:
            df = fetch(trade_type, code, ym)
            if not df.empty:
                frames.append(df)
            done += 1
            print(f"[{done}/{total}] {trade_type} {code} {ym} → {len(df)}건")
            time.sleep(delay)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    df = fetch_all("매매", ["11650"], ["202602"])
    print(df.head())