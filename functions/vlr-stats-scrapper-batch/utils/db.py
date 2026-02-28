from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from utils.vct_logging import logger
from utils.constants import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    POOL_MIN_CONN,
    POOL_MAX_CONN,
)


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
