
import getpass
from typing import List
from pathlib import Path
from datetime import date

import pandas as pd 

from sqlalchemy import Engine
from sqlalchemy import URL
from sqlalchemy import create_engine
import yfinance as yf




def sp500_symbols() -> List[str]:
    csv_file = Path('./index/sp500.csv')
    assert csv_file.exists()
    return pd.read_csv(csv_file)['Symbol'].map(str).to_list()


def get_db_engine() -> Engine:
    url_object = URL.create(
        "postgresql+psycopg",
        username=getpass.getuser(),      # return current system user
        password=getpass.getpass(prompt='Please enter Postgresql password for current system user.'),      
        host="localhost",
        database="finance",
    )
    return create_engine(url_object) 


def clean_yf_downloaded_df(df_raw: pd.DataFrame) -> pd.DataFrame:

    # flatten multil-level columns
    # https://stackoverflow.com/questions/63107594/how-to-deal-with-multi-level-column-names-downloaded-with-yfinance
    stocks_df = df_raw.stack(level=0, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1)

    # lots of null value as all stocks first trading date set to match the oldlest stock's first trading date
    df = stocks_df.dropna()

    # change from float to int
    df['Volume'] = df['Volume'].fillna(0).astype(int)

    # change Date from index to column
    df = df.reset_index() 

    # to match df columns to sql table stock_prices columns
    df = df.rename(columns={'Date': 'trade_date',    
                            'Ticker': 'ticker',
                            'Open': 'open_price',
                            'High': 'high_price',
                            'Low': 'low_price',
                            'Close': 'close_price',
                            'Volume': 'volume'
                            })
    
    df = df.rename_axis(' ', axis=1) # change column label from 'Price' to ' '
    
    df['trade_date'] = df['trade_date'].dt.date # change dtype from timestamp to date

    # remove today's stock data, as that could be incomplete if downloaded within trading hour
    if (to_day:= date.today()) == df['trade_date'].max():
        df_today = df[df['trade_date'] == to_day] 
        df = df.drop(index=df_today.index)
    
    return df 


def download_max(symbols: List[str]) -> pd.DataFrame:
    """download max stock price data for symbols
    
    start: first ever trade day of each symbol
    end: the day before today (to aviod in-tradeday incomplete data)

    NOTE: use this func to intially populate a sql table when records of symbols doesn't exist in table
    """
    stocks_df_raw = yf.download(tickers=symbols, group_by='Ticker', period='max', rounding=True)
    assert stocks_df_raw is not None

    df = clean_yf_downloaded_df(stocks_df_raw)
    return df


def download_from(symbols: List[str], from_date: str) -> pd.DataFrame:
    """download stock data from from_date to latest trade date
    
    NOTE: use this func to fetch new data for existing symbols in db
    from_date: YYYY-MM-DD
    """
    assert date.fromisoformat(from_date) < date.today()
    stocks_df_raw = yf.download(tickers=symbols, group_by='Ticker', start=from_date, end=date.today(), rounding=True)
    assert stocks_df_raw is not None

    df = clean_yf_downloaded_df(stocks_df_raw)
    return df 

