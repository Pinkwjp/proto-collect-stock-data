
from sqlalchemy.orm import declarative_base
from sqlalchemy import UniqueConstraint, Column, Integer, String, Date, Numeric, BigInteger
from sqlalchemy import text

from importlib import reload
from src import utils
reload(utils)

from src.utils import get_db_engine



Base = declarative_base()

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    __table_args__ = (UniqueConstraint('ticker', 'trade_date'),)  # tuple

    id = Column(Integer, primary_key=True)
    ticker = Column(String(10), nullable=False)
    trade_date = Column(Date, nullable=False)
    open_price = Column(Numeric(10, 2))
    high_price = Column(Numeric(10, 2))
    low_price = Column(Numeric(10, 2))
    close_price = Column(Numeric(10, 2))
    volume = Column(BigInteger)             # max volume is more than 9B


class SP500(Base):
    __tablename__ = 'sp500'
    __table_args__ = (UniqueConstraint('ticker', 'company_name'),)
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(10), nullable=False)
    company_name = Column(String(50), nullable=False)
    sector = Column(String(100), nullable=False)
    sub_industry = Column(String(100), nullable=False)
    headquarters = Column(String(50), nullable=False)
    date_added = Column(Date, nullable=False)
    cik = Column(Integer, nullable=False)
    year_founded = Column(Integer, nullable=False)


def create_tables():
    engine = get_db_engine()
    Base.metadata.create_all(engine)


def create_view_latest_trade_date():
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE OR REPLACE VIEW view_latest_trade_date AS
            SELECT ticker, MAX(trade_date) AS latest_trade_date
            FROM stock_prices
            GROUP BY ticker;
        """))
        conn.commit() 



if __name__ == '__main__':
    # python -m src.create_tables

    # create_tables()  
    # create_view_latest_trade_date()

    pass 


