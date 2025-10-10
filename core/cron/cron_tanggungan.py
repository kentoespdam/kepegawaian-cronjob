import time

import numpy as np
import pandas as pd

from core.config import LOGGER, log_duration
from core.enums import StatusPendidikan
from core.helper import cleanup_boolean_dynamic
from core.models.pegawai import update_jml_tanggungan_pegawai
from core.models.profil_keluarga import fetch_tanggungan, fetch_jml_tanggungan, update_tanggungan_profil_keluarga


class CronTanggungan:
    def __init__(self):
        self.start_time = time.time()
        self._df = pd.DataFrame()

    @property
    def data(self) -> pd.DataFrame:
        """Get processed data dengan type safety."""
        if self._df is None:
            return pd.DataFrame()
        return self._df

    def _finish(self):
        """Log execution duration."""
        log_duration("Cron Tanggungan", self.start_time)

    def execute(self, bulan: int):
        """Execute main cron job workflow."""
        LOGGER.info("Executing cron job tanggungan Anak")

        try:
            (
                self._load_data(bulan)
                ._process_boolean_columns()
                ._apply_business_rules()
                ._validate_data()
                ._save_results()
                ._update_pegawai_summary()
            )
        except Exception as e:
            LOGGER.error(f"Cron job failed: {e}")
            self._finish()
            raise

    def _load_data(self, bulan: int):
        """Load data tanggungan dari database."""
        self._df = fetch_tanggungan(bulan)
        LOGGER.info(f"Loaded {len(self.data)} records")
        return self

    def _process_boolean_columns(self) -> 'CronTanggungan':
        """Process semua kolom boolean sekaligus."""
        if self._df.empty:
            return self

        for col in {"status_kawin", "tanggungan", "lta_tag"}:
            if col in self._df.columns:
                self._df[col] = cleanup_boolean_dynamic(self._df[col])

        return self

    def _apply_business_rules(self) -> 'CronTanggungan':
        if self._df.empty:
            return self
        is_married = self._df["status_kawin"]
        is_adult = self._df["umur"].gt(19)
        is_not_selesai_sekolah = self._df["status_pendidikan"].ne(StatusPendidikan.SELESAI_SEKOLAH.value)
        is_married_or_adult = is_married | (is_adult & is_not_selesai_sekolah)
        self._df = self._df.assign(
            tanggungan=np.where(is_married_or_adult, False, self._df["tanggungan"]),
            lta_tag=np.where(is_married_or_adult, True, self._df["lta_tag"]),
        )

        return self

    def _validate_data(self) -> 'CronTanggungan':
        if self._df.empty:
            return self

        required_cols = ["id", "status_kawin", "umur", "tanggungan", "lta_tag"]
        missing_cols = [col for col in required_cols if col not in self._df.columns]

        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        return self

    def _save_results(self) -> 'CronTanggungan':
        if self._df.empty:
            return self

        update_tanggungan_profil_keluarga(self._df)

        return self

    def _update_pegawai_summary(self) -> None:
        """Update summary jumlah tanggungan per pegawai."""
        jml_tanggungan_per_pegawai_df = fetch_jml_tanggungan()
        if jml_tanggungan_per_pegawai_df.empty:
            self._finish()
            return

        update_jml_tanggungan_pegawai(jml_tanggungan_per_pegawai_df)
        self._finish()
