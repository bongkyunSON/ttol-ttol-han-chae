from datetime import date
from dateutil.relativedelta import relativedelta
from Pipeline.Storage import get_connection


def get_last_ym(trade_type: str) -> str | None:
    """
    DB에서 마지막으로 수집된 계약년월 반환
    trade_type: "매매" or "전월세"
    반환: "202602" 형식, 데이터 없으면 None
    """
    table = "trades" if trade_type == "매매" else "rentals"
    con = get_connection()
    result = con.execute(f"SELECT MAX(계약년월) FROM {table}").fetchone()[0]
    con.close()
    return result


def get_target_ym_list(trade_type: str, start_ym: str | None = None) -> list[str]:
    """
    수집 대상 월 목록 반환

    - start_ym 미지정: DB의 마지막 수집월 다음달 ~ 저번달
    - start_ym 지정  : 해당 월 ~ 저번달 (최초 수집 시 사용)

    예) 오늘이 2026-03-03, DB 마지막 수집월 = 202602
        → 반환: [] (202603은 아직 미공개)

    예) 오늘이 2026-04-10, DB 마지막 수집월 = 202602
        → 반환: ["202603"]
    """
    if start_ym:
        current = _parse_ym(start_ym)
    else:
        last = get_last_ym(trade_type)
        if last is None:
            raise ValueError(
                f"{trade_type} 테이블에 데이터가 없습니다. "
                "start_ym 을 직접 지정해주세요. 예: start_ym='202001'"
            )
        current = _parse_ym(last) + relativedelta(months=1)

    # 저번달까지만 수집 (이번달은 아직 미공개)
    last_available = date.today().replace(day=1) - relativedelta(months=1)

    ym_list = []
    while current <= last_available:
        ym_list.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    return ym_list


def _parse_ym(ym: str) -> date:
    """'202602' → date(2026, 2, 1)"""
    return date(int(ym[:4]), int(ym[4:6]), 1)
