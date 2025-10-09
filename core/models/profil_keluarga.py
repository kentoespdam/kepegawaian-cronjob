from typing import Optional

import pandas as pd

from core.config import fetch_data, save_update, LOGGER
from core.enums import StatusKerja, HubunganKeluarga


def fetch_tanggungan(bulan: int, pegawai_id: Optional[int] = None):
    params = (
        False,  # pegawai.is_deleted
        (StatusKerja.KARYAWAN_AKTIF.value, StatusKerja.DIRUMAHKAN.value,),  # pegawai_status_kerja
        False,  # profil_keluarga.is_deleted
        HubunganKeluarga.ANAK.value,  # profil_keluarga.hubungan_keluarga
        True,  # profil_keluarga.tanggungan
        bulan,  # profil_keluarga.tanggal_lahir (bulan lahir)
        20,  # profil_keluarga.tanggal_lahir (umur)
    )

    query = """
            SELECT p.id AS pegawai_id,
                   p.nipam,
                   pk.biodata_id,
                   pk.id,
                   pk.nama,
                   pk.status_kawin,
                   pk.status_pendidikan,
                   TIMESTAMPDIFF(
                           YEAR,
                           pk.tanggal_lahir,
                           CURRENT_DATE
                   )    AS umur,
                   pk.tanggungan,
                   pk.lta_tag
            FROM profil_keluarga pk
                     INNER JOIN pegawai p ON pk.biodata_id = p.nik AND p.is_deleted = %s AND p.status_kerja IN %s
            WHERE pk.is_deleted = %s
              AND pk.hubungan_keluarga = %s
              AND pk.tanggungan = %s
              AND MONTH(pk.tanggal_lahir) = %s
              AND TIMESTAMPDIFF(YEAR, pk.tanggal_lahir, CURRENT_DATE) >= %s
            ORDER BY pk.tanggal_lahir
            """

    if pegawai_id is not None:
        query += " AND p.id=%s"
        params += (pegawai_id,)
    return fetch_data(query, params)


def fetch_jml_tanggungan(biodata_ids: Optional[list] = None):
    params = (False, HubunganKeluarga.ANAK.value, True,)

    query = """
            SELECT pk.biodata_id,
                   COUNT(*) AS jml_tanggungan
            FROM profil_keluarga AS pk
            WHERE pk.is_deleted = %s
              AND pk.hubungan_keluarga = %s
              AND pk.tanggungan = %s
            GROUP BY biodata_id
            """
    if biodata_ids is not None:
        query += " AND pk.biodata_id IN %s"
        params += (biodata_ids,)
    return fetch_data(query, params)


def update_tanggungan_profil_keluarga(df: pd.DataFrame):
    data = [(
        row.tanggungan,
        row.lta_tag,
        row.id
    ) for row in df.itertuples()]

    query = "UPDATE profil_keluarga SET tanggungan=%s, lta_tag=%s WHERE id = %s"
    save_update(query, data)
