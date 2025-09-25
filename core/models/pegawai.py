import pandas as pd

from core.config import save_update, fetch_data
from core.enums import StatusKerja


def fetch_for_statistik():
    params = (False, (StatusKerja.KARYAWAN_AKTIF.value, StatusKerja.DIRUMAHKAN.value,))
    query = """SELECT peg.status_pegawai,
                      LOWER(org.category)                         AS category,
                      IF(bio.jenis_kelamin = 0, 'pria', 'wanita') AS jenis_kelamin,
                      jp.short_name                               AS pendidikan,
                      gol.golongan                                AS golongan
               FROM pegawai AS peg
                        INNER JOIN biodata AS bio ON peg.nik = bio.nik
                        LEFT JOIN golongan AS gol ON peg.golongan_id = gol.id
                        INNER JOIN jenjang_pendidikan AS jp ON bio.pendidikan_id = jp.id
                        INNER JOIN organisasi AS org ON peg.organisasi_id = org.id
               WHERE peg.is_deleted = %s
                 AND peg.status_kerja IN %s \
            """
    return fetch_data(query, params)


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
