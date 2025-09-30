from unittest import TestCase

import pandas as pd
from icecream import ic

from core import LOGGER
from core.cron.cron_tanggungan import CronTanggungan
from core import StatusKawin, StatusPendidikan
from core import update_jml_tanggungan_pegawai
from core.models.profil_keluarga import fetch_tanggungan, fetch_jml_tanggungan, \
    update_tanggungan_profil_keluarga


class Test(TestCase):
    def test_fetch_tanggungan(self):
        semua_tanggungan_df = fetch_tanggungan()
        self.assertGreater(len(semua_tanggungan_df), 0)
        ic(semua_tanggungan_df["id"].count())
        lepas_tanggungan_df = self.filter_data(semua_tanggungan_df)
        ic(lepas_tanggungan_df["id"].count())
        self.do_update_profil_keluarga(lepas_tanggungan_df)
        self.do_update_jml_tanggungan_pegawai(lepas_tanggungan_df)

    def filter_data(self, df: pd.DataFrame):
        df = df.copy()
        condition1 = df["status_kawin"].eq(StatusKawin.KAWIN.value)
        condition2 = df["umur"].ge(26)
        condition3 = df["umur"].gt(21)
        condition4 = df["status_pendidikan"].eq(StatusPendidikan.SELESAI_SEKOLAH.value)
        df = df[condition1 | condition2 | (condition3 & condition4)].reset_index(drop=True)
        self.assertGreater(df["id"].count(), 0)
        return df

    def do_update_profil_keluarga(self, df: pd.DataFrame):
        LOGGER.info(f"Executing update profil Keluarga {df['id'].count()} rows updated")
        self.assertGreater(df["id"].count(), 0)
        update_tanggungan_profil_keluarga(df)

    def do_update_jml_tanggungan_pegawai(self, df: pd.DataFrame):
        df = df.copy()
        biodata_ids = df["biodata_id"].unique().tolist()
        self.assertGreater(len(biodata_ids), 0)
        ic(biodata_ids)
        jml_tanggungan_per_pegawai_df = fetch_jml_tanggungan(biodata_ids)
        ic(jml_tanggungan_per_pegawai_df.to_dict("records"))
        update_jml_tanggungan_pegawai(jml_tanggungan_per_pegawai_df)

    def test_class_tanggungan(self):
        cron_tanggungan = CronTanggungan()
        cron_tanggungan.execute()
        self.assertEqual(True, True)
