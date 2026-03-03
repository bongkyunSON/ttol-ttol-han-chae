import json
from pathlib import Path

_JSON_PATH = Path(__file__).parent.parent / "Data" / "SigunguMapping.json"


def _load() -> dict:
    with open(_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_code(name: str) -> str | None:
    """
    구 이름 → 시군구코드 반환
    예: get_code("강남구") → "11680"
        get_code("송파구") → "11710"
    """
    data = _load()
    for sido, sigungu_dict in data.items():
        for code, sigungu_name in sigungu_dict.items():
            if sigungu_name == name:
                return code
    return None


def get_all_codes() -> list[str]:
    """
    SigunguMapping.json 에 있는 모든 시군구코드 반환
    파이프라인에서 수도권 전체 수집 시 사용
    """
    data = _load()
    codes = []
    for sigungu_dict in data.values():
        codes.extend(sigungu_dict.keys())
    return codes


def get_codes_by_sido(sido: str) -> list[str]:
    """
    특정 시도의 시군구코드 목록 반환
    예: get_codes_by_sido("서울특별시") → ["11110", "11140", ...]
        get_codes_by_sido("경기도")
        get_codes_by_sido("인천시")
    """
    data = _load()
    if sido not in data:
        raise ValueError(f"'{sido}' 는 SigunguMapping.json 에 없습니다. 서울특별시 / 경기도 / 인천시 중 하나를 입력하세요.")
    return list(data[sido].keys())
