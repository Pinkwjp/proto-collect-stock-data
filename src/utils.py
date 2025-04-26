
from typing import List
from pathlib import Path

import pandas as pd 



def sp500_symbols() -> List[str]:
    csv_file = Path('./index/sp500.csv')
    assert csv_file.exists()
    return pd.read_csv(csv_file)['Symbol'].map(str).to_list()



