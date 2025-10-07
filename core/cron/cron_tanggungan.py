import time

import numpy as np
import pandas as pd

from core.config import LOGGER, log_duration
from core.enums import StatusPendidikan
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
        df["tanggungan"] = self._cleanup_tanggungan(
            df["tanggungan"],
            df["status_kawin"],
            df["umur"],
            df["status_pendidikan"]
        )
        update_tanggungan_profil_keluarga(df)
        self._update_jml_tanggungan_pegawai()

    @staticmethod
    def _cleanup_tanggungan(tanggungan: pd.Series, status_kawin: pd.Series, umur: pd.Series,
                            status_pendidikan: pd.Series):
        mask_status_kawin = status_kawin.eq(True)
        mask_umur_26 = umur.ge(26)
        mask_umur_gt_21 = umur.gt(21)
        mask_status_pendidikan = status_pendidikan.ne(StatusPendidikan.SEKOLAH.value)
        result = np.select([mask_status_kawin | mask_umur_26 | (mask_umur_gt_21 & mask_status_pendidikan)], [False],
                           default=tanggungan)
        return result

    def _update_jml_tanggungan_pegawai(self):
        jml_tanggungan_per_pegawai_df = fetch_jml_tanggungan()
        update_jml_tanggungan_pegawai(jml_tanggungan_per_pegawai_df)
        self._finish()
