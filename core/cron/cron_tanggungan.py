import time

import pandas as pd

from core.config import LOGGER, log_duration
from core.enums import StatusKawin, StatusPendidikan
from core.models.pegawai import update_jml_tanggungan_pegawai
from core.models.profil_keluarga import fetch_tanggungan, update_tanggungan_profil_keluarga, \
    fetch_jml_tanggungan_by_biodata_ids


class CronTanggungan:
    def __init__(self):
        self.start_time = time.time()
        self.all_tanggungan_df = pd.DataFrame()
        self.lepas_tanggungan_df = pd.DataFrame()

    def _finish(self):
        log_duration("Cron Tanggungan", self.start_time)

    def execute(self):
        LOGGER.info("Executing cron job tanggungan Anak")
        self.all_tanggungan_df = fetch_tanggungan()
        if self.all_tanggungan_df.empty:
            self._finish()
            return
        self._update_status_tanggungan()

    def _update_status_tanggungan(self):
        self._filter_data()
        if self.lepas_tanggungan_df.empty:
            self._finish()
            return
        update_tanggungan_profil_keluarga(self.lepas_tanggungan_df)
        self._update_jml_tanggungan_pegawai()

    def _filter_data(self):
        df = self.all_tanggungan_df.copy()
        cond1 = df["status_kawin"].eq(StatusKawin.KAWIN.value)
        cond2 = df["umur"].ge(26)
        cond3 = df["umur"].gt(21)
        cond4 = df["status_pendidikan"].ne(StatusPendidikan.SEKOLAH.value)
        df = df[cond1 | cond2 | (cond3 & cond4)].reset_index(drop=True)
        self.lepas_tanggungan_df = df

    def _update_jml_tanggungan_pegawai(self):
        df = self.lepas_tanggungan_df.copy()
        biodata_ids = df["biodata_id"].unique().tolist()
        if len(biodata_ids) == 0:
            self._finish()
            return
        jml_tanggungan_per_pegawai_df = fetch_jml_tanggungan_by_biodata_ids(biodata_ids)
        update_jml_tanggungan_pegawai(jml_tanggungan_per_pegawai_df)
        self._finish()
