import pandas as pd

from core.config import get_connection_pool, LOGGER
from core.enums import StatusKerja, HubunganKeluarga


def fetch_tanggungan(pegawai_id: int | None = None):
    query = """
            SELECT p.id AS pegawai_id,
                   pk.id,
                   pk.status_kawin,
                   pk.status_pendidikan,
                   TIMESTAMPDIFF(
                           YEAR,
                           pk.tanggal_lahir,
                           CURRENT_DATE
                   )    AS umur
            FROM profil_keluarga pk
                     INNER JOIN pegawai p
                                ON pk.biodata_id = p.nik
                                    AND p.is_deleted = FALSE
                                    AND p.status_kerja IN (%s, %s)
            WHERE pk.is_deleted = FALSE
              AND pk.hubungan_keluarga = %s
              AND pk.tanggungan = %s
            """
    params = (StatusKerja.KARYAWAN_AKTIF.value,
              StatusKerja.DIRUMAHKAN.value,
              HubunganKeluarga.ANAK.value,
              True)

    if pegawai_id:
        query += " AND p.id=%s"
        params = params + (pegawai_id,)

    with get_connection_pool() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return pd.DataFrame(cursor.fetchall())


def update_tanggungan_status(df: pd.DataFrame):
    data = [(
        row.tanggungan,
        row.id
    ) for row in df.itertuples()]

    query = "UPDATE profil_keluarga SET tanggungan=%s WHERE id = %s"
    with get_connection_pool() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.executemany(query, data)
                LOGGER.info(f"{conn.affected_rows()} rows affected")
                conn.commit()
            except Exception as e:
                conn.rollback()
                LOGGER.error(e)
