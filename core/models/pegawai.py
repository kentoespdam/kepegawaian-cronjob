import pandas as pd

from core.config import save_update


def update_jml_tanggungan_pegawai(df: pd.DataFrame):
    data = [(
        row.jml_tanggungan,
        row.biodata_id
    ) for row in df.itertuples(index=False)]
    query = """
            UPDATE pegawai
            SET jml_tanggungan=%s
            WHERE nik = %s \
            """
    save_update(query, data)
