import os
import json
import pandas as pd
from PublicDataReader import TransactionPrice

from dotenv import load_dotenv

load_dotenv()


service_key = os.getenv("MOLTI_API_KEY")
api = TransactionPrice(service_key)

df = api.get_data(
    property_type="아파트",
    trade_type="매매",
    sigungu_code="11650",
    start_year_month="202201",
    end_year_month="202212",
)

df.tail()


print(df.head())
