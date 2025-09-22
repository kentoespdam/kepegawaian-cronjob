import time

from dotenv import load_dotenv
import os
import logging

from pymysqlpool import Connection, DictCursor, ConnectionPool

load_dotenv()
logging.basicConfig(
    datefmt="%Y-%m-%d %H:%M:%S", format="%(asctime)s %(levelname)s %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO")
)
LOGGER = logging.getLogger(__name__)


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
