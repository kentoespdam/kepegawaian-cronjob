import time

import pandas as pd

from core.config import LOGGER, log_duration
from core.enums import StatusKawin, StatusPendidikan
from core.models.pegawai import update_jml_tanggungan_pegawai
from core.models.profil_keluarga import fetch_tanggungan, update_tanggungan_profil_keluarga, \
    fetch_jml_tanggungan


class CronTanggungan:
    def __init__(self):
        self.start_time = time.time()
        self.all_tanggungan_df = pd.DataFrame()
        self.lepas_tanggungan_df = pd.DataFrame()

    def _finish(self):
        log_duration("Cron Tanggungan", self.start_time)

    def execute(self):
        LOGGER.info("Executing cron job tanggungan Anak")
        df = fetch_tanggungan()
        df["tanggungan"] = df.apply(lambda x: self._set_tanggungan(x), axis=1)
        update_tanggungan_profil_keluarga(df)
        self._update_jml_tanggungan_pegawai()

    @staticmethod
    def _set_tanggungan(row: pd.Series):
        """Set tanggungan status based on conditions."""
        status_kawin = row["status_kawin"] == StatusKawin.KAWIN.value
        umur_ge_26 = row["umur"] >= 26
        umur_gt_21 = row["umur"] > 21
        status_pendidikan_neq_sekolah = row["status_pendidikan"] != StatusPendidikan.SEKOLAH.value
        return False if status_kawin or umur_ge_26 or (umur_gt_21 and status_pendidikan_neq_sekolah) else True

    def _update_jml_tanggungan_pegawai(self):
        jml_tanggungan_per_pegawai_df = fetch_jml_tanggungan()
        update_jml_tanggungan_pegawai(jml_tanggungan_per_pegawai_df)
        self._finish()