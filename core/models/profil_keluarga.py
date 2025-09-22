import pandas as pd

from core.config import fetch_data, save_update
from core.enums import StatusKerja, HubunganKeluarga


def fetch_tanggungan(pegawai_id: int | None = None):
    params = ([StatusKerja.KARYAWAN_AKTIF.value,
               StatusKerja.DIRUMAHKAN.value],
              False,
              HubunganKeluarga.ANAK.value,
              True,
              )

    query = """
            SELECT p.id AS pegawai_id,
                   pk.biodata_id,
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
                                    AND p.status_kerja IN %s
            WHERE pk.is_deleted = %s
              AND pk.hubungan_keluarga = %s
              AND pk.tanggungan = %s
            """

    if pegawai_id is not None:
        query += " AND p.id=%s"
        params += (pegawai_id,)

    return fetch_data(query, params)


def fetch_jml_tanggungan_by_biodata_ids(biodata_ids: list):
    params = (False, HubunganKeluarga.ANAK.value, True, biodata_ids,)

    query = """
            SELECT pk.biodata_id,
                   COUNT(*) AS jml_tanggungan
            FROM profil_keluarga AS pk
            WHERE pk.is_deleted = %s
              AND pk.hubungan_keluarga = %s
              AND pk.tanggungan = %s
              AND pk.biodata_id IN %s
            GROUP BY biodata_id"""
    return fetch_data(query, params)


def update_tanggungan_profil_keluarga(df: pd.DataFrame):
    data = [(
        False,
        row.id
    ) for row in df.itertuples()]

    query = "UPDATE profil_keluarga SET tanggungan=%s WHERE id = %s"
    save_update(query, data)
