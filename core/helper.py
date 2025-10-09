from typing import List, Optional, Any

import pandas as pd


def cleanup_boolean_dynamic(series: pd.Series,
                            true_values: Optional[List[Any]] = None,
                            false_values: Optional[List[Any]] = None,
                            target_dtype: str = 'bool') -> pd.Series:
    """
    Cleanup boolean series dengan format dinamis (1,0, b'\\x00', b'\\x01', dll).

    Parameters:
    -----------
    series : pd.Series
        Series yang akan dibersihkan
    true_values : list, optional
        Nilai-nilai yang dianggap True
    false_values : list, optional
        Nilai-nilai yang dianggap False
    target_dtype : str, default 'bool'
        Tipe data target: 'bool', 'int', 'str'

    Returns:
    --------
    pd.Series
        Series yang sudah dibersihkan
    """
    # Default values untuk konversi boolean
    default_true_values = [1, True, '1', 'True', 'true', 'YES', 'Yes', 'yes', b'\x01']
    default_false_values = [0, False, '0', 'False', 'false', 'NO', 'No', 'no', b'\x00']

    true_vals = true_values if true_values is not None else default_true_values
    false_vals = false_values if false_values is not None else default_false_values

    # Buat mapping dictionary
    value_map = {}
    for val in true_vals:
        value_map[val] = True
    for val in false_vals:
        value_map[val] = False

    # Konversi series
    cleaned_series = series.map(value_map)

    # Handle values yang tidak ada di mapping
    mask_unmapped = cleaned_series.isna() & series.notna()
    if mask_unmapped.any():
        print(f"Warning: {mask_unmapped.sum()} values tidak dapat dikonversi: {series[mask_unmapped].unique()}")
        cleaned_series = cleaned_series.fillna(False)

    # Konversi ke target dtype
    return _convert_to_target_dtype(cleaned_series, target_dtype)


def _convert_to_target_dtype(series: pd.Series, target_dtype: str) -> pd.Series:
    """Konversi series ke tipe data target."""
    if target_dtype == 'bool':
        return series.astype(bool)
    elif target_dtype == 'int':
        return series.astype(int)
    elif target_dtype == 'str':
        return series.astype(str)
    else:
        raise ValueError(f"Target dtype {target_dtype} tidak didukung. Gunakan 'bool', 'int', atau 'str'")
