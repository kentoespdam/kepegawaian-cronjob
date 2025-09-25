import pandas as pd

from core.config import save_update


def save_statistik_pegawai(df: pd.DataFrame):
    data = [(
        row.seq,
        row.pendidikan,
        row.tahun,
        row.bulan,
        row.non_golongan,
        row.golongan_a,
        row.golongan_b,
        row.golongan_c,
        row.golongan_d,
        row.kontrak,
        row.capeg,
        row.honorer,
        row.tetap,
        row.adm,
        row.pelayanan,
        row.teknik,
        row.pria,
        row.wanita
    ) for row in df.itertuples(index=False)]

    query = """
            INSERT INTO statistik_pegawai(seq, pendidikan, tahun, bulan, non_golongan, golongan_a, golongan_b,
                                          golongan_c, golongan_d, kontrak, capeg, honorer, tetap, adm, pelayanan,
                                          teknik, pria, wanita)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE non_golongan=VALUES(non_golongan),
                                    golongan_a=VALUES(golongan_a),
                                    golongan_b=VALUE(golongan_b),
                                    golongan_c=VALUES(golongan_c),
                                    golongan_d=VALUES(golongan_d),
                                    kontrak=VALUES(kontrak),
                                    capeg=VALUES(capeg),
                                    honorer=VALUES(honorer),
                                    tetap=VALUES(tetap),
                                    adm=VALUES(adm),
                                    pelayanan=VALUES(pelayanan),
                                    teknik=VALUES(teknik),
                                    pria=VALUES(pria),
                                    wanita=VALUES(wanita)
            """

    save_update(query, data)
