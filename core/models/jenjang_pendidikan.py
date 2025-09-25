from core.config import fetch_data


def fetch_jenjang_pendidikan(is_statistik: bool | None = None):
    params = (False,)
    query = "SELECT id, seq, short_name AS pendidikan, is_statistik FROM jenjang_pendidikan WHERE is_deleted=%s"
    if is_statistik is not None:
        query += " AND is_statistik=%s"
        params += (is_statistik,)
    return fetch_data(query, params)
