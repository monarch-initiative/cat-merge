import pandas as pd
from io import StringIO


# Borrowed from https://stackoverflow.com/questions/58771331/cleanly-hard-code-a-pandas-dataframe-into-a-python-script
def string_df(data: str, index_column_is_id=True):
    if index_column_is_id:
        df = pd.read_csv(StringIO(data), index_col='id', sep=r"\s+", engine='python')
    else:
        df = pd.read_csv(StringIO(data), sep=r"\s+", engine='python')
    return df
