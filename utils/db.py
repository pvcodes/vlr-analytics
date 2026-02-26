import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from utils.vct_logging import logger


# =========================================================
# ENV
# =========================================================

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

POOL_MIN_CONN = int(os.environ.get("DB_POOL_MIN_CONN", 1))
POOL_MAX_CONN = int(os.environ.get("DB_POOL_MAX_CONN", 10))


# =========================================================
# CONNECTION POOL
# =========================================================

try:
    pool = ThreadedConnectionPool(
        POOL_MIN_CONN,
        POOL_MAX_CONN,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    logger.info("Postgres connection pool created")

except Exception as e:
    logger.exception("Failed to create Postgres pool")
    raise


# =========================================================
# CONTEXT MANAGER
# =========================================================


@contextmanager
def get_conn():

    conn = pool.getconn()

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# =========================================================
# DB HOOK
# =========================================================


class DBHook:

    def get_records(self, query: str, parameters=None):

        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                return cursor.fetchall()

    def run(self, query: str, parameters=None):

        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                return cursor.rowcount


# =========================================================
# FACTORY
# =========================================================


def get_db_hook() -> DBHook:
    return DBHook()
