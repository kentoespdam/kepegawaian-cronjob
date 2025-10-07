from enum import Enum


class StatusPegawai(Enum):
    KONTRAK = 0
    CAPEG = 1
    PEGAWAI = 2
    CALON_HONORER = 3
    HONORER = 4
    NON_PEGAWAI = 5


class StatusKerja(Enum):
    BERHENTI_OR_KELUAR = 0
    DIRUMAHKAN = 1
    KARYAWAN_AKTIF = 2
    LAMARAN_BARU = 3
    TAHAP_SELEKSI = 4
    DITERIMA = 5
    DIREKOMENDASIKAN = 6
    DITOLAK = 7


class HubunganKeluarga(Enum):
    SUAMI = 0
    ISTRI = 1
    AYAH = 2
    IBU = 3
    ANAK = 4
    SAUDARA = 5


class StatusPendidikan(Enum):
    BELUM_SEKOLAH = 0
    SEKOLAH = 1
    SELESAI_SEKOLAH = 2


class Tunjangan(Enum):
    JABATAN = 0
    KINERJA = 1
    BERAS = 2
    AIR = 3
