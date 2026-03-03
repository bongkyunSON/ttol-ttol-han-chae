import pandas as pd

# 매매 API 태그 → 한국어 컬럼명
TRADE_RENAME = {
    "sggCd":              "시군구코드",
    "umdNm":              "법정동",
    "jibun":              "번지",
    "roadNmBonbun":       "본번",
    "roadNmBubun":        "부번",
    "aptNm":              "단지명",
    "excluUseAr":         "전용면적(㎡)",
    "dealYear":           "_년",    # 임시 컬럼 → 아래에서 _월과 합쳐 계약년월로 변환 후 삭제
    "dealMonth":          "_월",    # 임시 컬럼 → 동일
    "dealDay":            "계약일",
    "dealAmount":         "거래금액(만원)",
    "floor":              "층",
    "buyerGbn":           "매수자",
    "slerGbn":            "매도자",
    "buildYear":          "건축년도",
    "roadNm":             "도로명",
    "cdealDay":           "해제사유발생일",
    "dealingGbn":         "거래유형",
    "estateAgentSggNm":   "중개사소재지",
    "rgstDate":           "등기일자",
}

# 전월세 API 태그 → 한국어 컬럼명
RENT_RENAME = {
    "sggCd":          "시군구코드",
    "umdNm":          "법정동",
    "jibun":          "번지",
    "aptNm":          "단지명",
    "leaseType":      "전월세구분",
    "excluUseAr":     "전용면적(㎡)",
    "dealYear":       "_년",    # 임시 컬럼 → 아래에서 _월과 합쳐 계약년월로 변환 후 삭제
    "dealMonth":      "_월",    # 임시 컬럼 → 동일
    "dealDay":        "계약일",
    "deposit":        "보증금(만원)",
    "monthlyRent":    "월세금(만원)",
    "floor":          "층",
    "buildYear":      "건축년도",
    "roadNm":         "도로명",
    "contractTerm":   "계약기간",
    "contractType":   "계약구분",
    "useRRRight":     "갱신요구권 사용",
    "preDeposit":     "종전계약 보증금(만원)",
    "preMonthlyRent": "종전계약 월세(만원)",
}


def transform(df: pd.DataFrame, trade_type: str) -> pd.DataFrame:
    """
    collector 에서 받은 영어 컬럼 DataFrame 을 한국어 컬럼으로 변환

    trade_type: "매매" or "전월세"
    """
    if df.empty:
        return df

    rename_map = TRADE_RENAME if trade_type == "매매" else RENT_RENAME
    df = df.rename(columns=rename_map)

    # dealYear + dealMonth → 계약년월 (예: "2026" + "2" → "202602")
    if "_년" in df.columns and "_월" in df.columns:
        df["계약년월"] = df["_년"] + df["_월"].str.zfill(2)
        df = df.drop(columns=["_년", "_월"])

    # 매핑에 없는 불필요한 영어 컬럼 제거
    defined_cols = set(rename_map.values()) - {"_년", "_월"} | {"계약년월"}
    extra_cols = [c for c in df.columns if c not in defined_cols]
    if extra_cols:
        df = df.drop(columns=extra_cols)

    # 주택유형 컬럼 추가 (API 응답에 없음 → 아파트 고정)
    df["주택유형"] = "아파트"

    return df.reset_index(drop=True)
