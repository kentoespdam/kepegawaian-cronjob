import time

import pandas as pd
from icecream import ic

from core.config import LOGGER, log_duration
from core.enums import StatusPegawai
from core.helper import cleanup_bool
from core.models.jenjang_pendidikan import fetch_jenjang_pendidikan
from core.models.pegawai import fetch_for_statistik
from core.models.statistik_pegawai import save_statistik_pegawai


class StatistikData:
    def __init__(self, tahun, bulan, seq, pendidikan):
        self.tahun = 0
        self.bulan = 0
        self.seq = 0
        self.pendidikan = 0
        self.non_golongan = 0
        self.golongan_a = 0
        self.golongan_b = 0
        self.golongan_c = 0
        self.kontrak = 0
        self.capeg = 0
        self.honorer = 0
        self.tetap = 0
        self.adm = 0
        self.pelayanan = 0
        self.teknik = 0
        self.pria = 0
        self.wanita = 0


class CronStatistikPegawai:
    def __init__(self):
        self.start_time = time.time()
        self.data_df = pd.DataFrame()
        self.statistik_df = pd.DataFrame()
        self.result_df = pd.DataFrame()

    def execute(self, tahun, bulan):
        LOGGER.info("Starting Cronjob Statistik Pegawai")
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
        self.statistik_df["golongan_category"] = self.statistik_df["golongan"].apply(self._categorize_golongan)
        self.statistik_df["status_pegawai_category"] = self.statistik_df["status_pegawai"].apply(
            _categorize_status_pegawai)

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
    def _categorize_golongan(golongan):
        if golongan.startswith("A"):
            return "golongan_a"
        elif golongan.startswith("B"):
            return "golongan_b"
        elif golongan.startswith("C"):
            return "golongan_c"
        elif golongan.startswith("D"):
            return "golongan_d"
        else:
            return "non_golongan"


def _get_jumlah(result_df, statistik_df):
    golongan_column_groups = ["non_golongan", "golongan_a", "golongan_b", "golongan_c", "golongan_d"]
    status_pegawai_column_groups = ["kontrak", "capeg", "honorer", "tetap"]
    profesi_column_groups = ["adm", "pelayanan", "teknik"]
    jenis_kelamin_column_groups = ["pria", "wanita"]

    # ic(result_df[["pendidikan"] + golongan_column_groups])

    # Initialize columns
    for col in golongan_column_groups + status_pegawai_column_groups + profesi_column_groups + jenis_kelamin_column_groups:
        if col not in result_df.columns:
            result_df[col] = 0

    # Precompute categories
    statistik_df["golongan_category"] = statistik_df["golongan"].apply(_categorize_golongan)
    statistik_df["status_pegawai_category"] = statistik_df["status_pegawai"].apply(_categorize_status_pegawai)

    # Buat pivot tables
    golongan_pivot = pd.crosstab(statistik_df['pendidikan'], statistik_df['golongan_category'])
    status_pivot = pd.crosstab(statistik_df['pendidikan'], statistik_df['status_pegawai_category'])
    profesi_pivot = pd.crosstab(statistik_df['pendidikan'], statistik_df['category'])
    jenis_kelamin_pivot = pd.crosstab(statistik_df['pendidikan'], statistik_df['jenis_kelamin'])

    # Gabungkan pivot tables
    combined_pivot = golongan_pivot.join(status_pivot, how='outer').join(profesi_pivot, how='outer').join(
        jenis_kelamin_pivot, how='outer').fillna(0)

    # Update result_df menggunakan vectorized operations
    for col in combined_pivot.columns:
        if col in result_df.columns:
            # Buat mapping dan update sekaligus
            mapping = combined_pivot[col].to_dict()
            result_df[col] += result_df['pendidikan'].map(mapping).fillna(0).astype(int)

    return result_df


def _categorize_golongan(golongan):
    if golongan.startswith("A"):
        return "golongan_a"
    elif golongan.startswith("B"):
        return "golongan_b"
    elif golongan.startswith("C"):
        return "golongan_c"
    elif golongan.startswith("D"):
        return "golongan_d"
    else:
        return "non_golongan"


def _categorize_status_pegawai(status_pegawai):
    if status_pegawai == StatusPegawai.KONTRAK.value:
        return "kontrak"
    elif status_pegawai == StatusPegawai.CAPEG.value:
        return "capeg"
    elif status_pegawai == StatusPegawai.HONORER.value or status_pegawai == StatusPegawai.CALON_HONORER.value:
        return "honorer"
    elif status_pegawai == StatusPegawai.PEGAWAI.value:
        return "tetap"
    else:
        return None


if __name__ == "__main__":
    cron = CronStatistikPegawai()
    cron.execute(2025, 8)
