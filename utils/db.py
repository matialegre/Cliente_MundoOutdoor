"""Helper de base de datos SQL Server via pyodbc."""
from __future__ import annotations

import pyodbc
from typing import Any, Sequence

from utils import config
from utils.logger import get_logger

log = get_logger(__name__)


def _get_conn() -> pyodbc.Connection:
    conn_str = config.SQL_CONN_STR or (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=ranchoaspen\\zoo2025;"  # Default server; sobreescribe con SQL_CONN_STR si es distinto
        "DATABASE=dragonfish_deposito;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def fetchone(query: str, params: Sequence[Any] | None = None) -> pyodbc.Row | None:
    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(query, params or [])
        return cur.fetchone()
