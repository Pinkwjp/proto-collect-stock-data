
import getpass
from typing import List
from pathlib import Path

import pandas as pd 
from sqlalchemy import Engine
from sqlalchemy import URL
from sqlalchemy import create_engine



def sp500_symbols() -> List[str]:
    csv_file = Path('./index/sp500.csv')
    assert csv_file.exists()
    return pd.read_csv(csv_file)['Symbol'].map(str).to_list()


def get_db_engine() -> Engine:
    url_object = URL.create(
        "postgresql+psycopg",
        username=getpass.getuser(),      # return current system user
        password=getpass.getpass(prompt='Please enter Postgresql password for current system user:'),      
        host="localhost",
        database="finance",
    )
    return create_engine(url_object) 
