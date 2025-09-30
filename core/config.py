import logging
import logging.config
import os
import time
import yaml

import pandas as pd
from dotenv import load_dotenv
from pymysql.cursors import DictCursor
from pymysqlpool import Connection, ConnectionPool

from core.cron_log import setup_logging

load_dotenv()

setup_logging()
LOGGER = logging.getLogger('cronjob')
schedule_logger = logging.getLogger("schedule")


def get_connection_pool() -> Connection:
    config = {
        "size": 10,
        "maxsize": 15,
        "pre_create_num": 2,
        "name": "kepegawaian-pool",
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT') or 3306),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASS'),
        'database': os.getenv('DB_NAME'),
        'charset': 'utf8mb4',
        'cursorclass': DictCursor,
    }

    return ConnectionPool(**config).get_connection()


def log_duration(value: str, start_time: float):
    LOGGER.info(f"{value} finish in {time.time() - start_time:.2f}ms")


def fetch_data(query: str, params: tuple | None = None) -> pd.DataFrame:
    with get_connection_pool() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description] if cursor.description else None
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)


def save_update(query: str, data: list):
    with get_connection_pool() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.executemany(query, data)
                affected = cursor.rowcount
                LOGGER.info(f"{affected} rows affected")
                conn.commit()
            except Exception as e:
                conn.rollback()
                LOGGER.error(e)


def save_update_single(query: str, data: tuple):
    with get_connection_pool() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query, data)
                affected = cursor.rowcount
                LOGGER.info(f"{affected} rows affected")
                conn.commit()
            except Exception as e:
                conn.rollback()
                LOGGER.error(e)
