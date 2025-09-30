import time
from datetime import datetime

import pandas as pd

from core.config import LOGGER, log_duration
from core.enums import StatusPegawai
from core.helper import cleanup_bool
from core.models.jenjang_pendidikan import fetch_jenjang_pendidikan
from core.models.pegawai import fetch_for_statistik
from core.models.statistik_pegawai import save_statistik_pegawai


class CronStatistikPegawai:
    def __init__(self):
        self.start_time = time.time()
        self.data_df = pd.DataFrame()
        self.statistik_df = pd.DataFrame()
        self.result_df = pd.DataFrame()

    def execute(self):
        LOGGER.info("Starting Cronjob Statistik Pegawai")
        now = datetime.now()
        tahun = now.year
        bulan = now.month
        self._initialize_result_df(tahun, bulan)
        self._initialize_statistik_df()
        self._calculate()

        save_statistik_pegawai(self.result_df)
        log_duration("Cronjob Statistik Pegawai", self.start_time)

    def _initialize_result_df(self, tahun, bulan):
        df = fetch_jenjang_pendidikan(True)
        df["is_statistik"] = cleanup_bool(df["is_statistik"])

        self.result_df = pd.DataFrame(df[["seq", "pendidikan"]],
                                      columns=["seq", "pendidikan", "tahun", "bulan", "non_golongan", "golongan_a",
                                               "golongan_b", "golongan_c", "golongan_d", "kontrak", "capeg", "honorer",
                                               "tetap", "adm", "pelayanan", "teknik", "pria", "wanita"])
        self.result_df[["non_golongan", "golongan_a",
                        "golongan_b", "golongan_c", "golongan_d", "kontrak", "capeg", "honorer", "tetap",
                        "adm", "pelayanan", "teknik", "pria", "wanita"]] = 0
        self.result_df["tahun"] = tahun
        self.result_df["bulan"] = bulan

    def _initialize_statistik_df(self):
        self.statistik_df = fetch_for_statistik()
        self.statistik_df["golongan"] = self.statistik_df["golongan"].apply(lambda x: x if x is not None else "")

    def _calculate(self):
        golongan_column_groups = ["non_golongan", "golongan_a", "golongan_b", "golongan_c", "golongan_d"]
        status_pegawai_column_groups = ["kontrak", "capeg", "honorer", "tetap"]
        # Initialize columns
        for col in golongan_column_groups + status_pegawai_column_groups:
            if col not in self.result_df.columns:
                self.result_df[col] = 0

        # Precompute categories
        self.statistik_df["golongan_category"] = self.statistik_df["golongan"].apply(self.categorize_golongan)
        self.statistik_df["status_pegawai_category"] = self.statistik_df["status_pegawai"].apply(
            self.categorize_status_pegawai)

        # Buat pivot tables
        golongan_pivot = pd.crosstab(self.statistik_df['pendidikan'], self.statistik_df['golongan_category'])
        status_pivot = pd.crosstab(self.statistik_df['pendidikan'], self.statistik_df['status_pegawai_category'])
        profesi_pivot = pd.crosstab(self.statistik_df['pendidikan'], self.statistik_df['category'])
        jenis_kelamin_pivot = pd.crosstab(self.statistik_df['pendidikan'], self.statistik_df['jenis_kelamin'])

        # Gabungkan pivot tables
        combined_pivot = golongan_pivot.join(status_pivot, how='outer').join(profesi_pivot, how='outer').join(
            jenis_kelamin_pivot, how='outer').fillna(0)

        # Update result_df menggunakan vectorized operations
        for col in combined_pivot.columns:
            if col in self.result_df.columns:
                # Buat mapping dan update sekaligus
                mapping = combined_pivot[col].to_dict()
                self.result_df[col] += self.result_df['pendidikan'].map(mapping).fillna(0).astype(int)

    @staticmethod
    def categorize_golongan(golongan: str) -> str:
        """
        Categorize golongan into its respective category.

        Args:
            golongan (str): The golongan to be categorized.

        Returns:
            str: The category of the golongan.
        """
        categories = {
            "A": "golongan_a",
            "B": "golongan_b",
            "C": "golongan_c",
            "D": "golongan_d"
        }

        for prefix, category in categories.items():
            if golongan.startswith(prefix):
                return category
        return "non_golongan"

    @staticmethod
    def categorize_status_pegawai(status: int) -> str:
        """
        Categorize status_pegawai into its respective category.

        Args:
            status (int): The status_pegawai to be categorized.

        Returns:
            str: The category of the status_pegawai.
        """
        categories = {
            StatusPegawai.KONTRAK.value: "kontrak",
            StatusPegawai.CAPEG.value: "capeg",
            StatusPegawai.HONORER.value: "honorer",
            StatusPegawai.CALON_HONORER.value: "honorer",
            StatusPegawai.PEGAWAI.value: "tetap"
        }
        return categories.get(status, None)

