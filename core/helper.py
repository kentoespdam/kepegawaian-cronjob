import pandas as pd


def cleanup_bool(df:pd.Series):
    return df.eq(b'\x01')