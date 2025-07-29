from app.db_route_handler.mysql_handler import MySQLHandler
from app.db_route_handler.postgres_handler import PostgresHandler
from app.utils.config import  TARGET_DB


def get_db_handler():
    if TARGET_DB == "POSTGRES":
        return PostgresHandler()
    elif TARGET_DB == "MYSQL":
        return MySQLHandler()
    else:
        raise ValueError(f"Unsupported TARGET_DB: {TARGET_DB}")
