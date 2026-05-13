import os
import sqlalchemy

_engine: sqlalchemy.Engine | None = None


def _get_engine() -> sqlalchemy.Engine:
    global _engine
    if _engine is None:
        url = "postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{HOST}:{INTERNAL_PORT}/bf4".format_map(os.environ)
        _engine = sqlalchemy.create_engine(url, pool_pre_ping=True)
    return _engine


def get_conn():
    return _get_engine().begin()
