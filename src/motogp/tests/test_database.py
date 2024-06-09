import sqlite3
from typing import assert_type

import duckdb
from motogp.database import setup_duckdb, setup_sqlite


def test_setup_duckdb():
    assert_type(setup_duckdb(), duckdb.DuckDBPyConnection)


def test_refresh_duckdb():
    conn = setup_duckdb(True)
    res = conn.execute("show").fetchall()
    assert len(res) == 7


def test_setup_sqlite():
    assert_type(setup_sqlite(), sqlite3.Connection)


def test_refresh_sqlite():
    conn = setup_sqlite(True)
    res = conn.execute("SELECT * FROM sqlite_master WHERE type = 'table'").fetchall()
    assert len(res) == 1
