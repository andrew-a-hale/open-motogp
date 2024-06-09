import os
import duckdb
import sqlite3


def setup_sqlite(refresh: bool = False):
    conn = sqlite3.connect("processing.db")

    if refresh:
        refresh_sqlite(conn)

    return conn


def refresh_sqlite(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tasks")

    path = os.path.dirname(__file__)
    files = os.listdir(path + "/sqlite")
    files.sort()
    for file in files:
        sql = open(path + "/sqlite/" + file).read()
        conn.execute(sql)

    cur.close()


def setup_duckdb(refresh: bool = False):
    conn = duckdb.connect("motogp.db")

    if refresh:
        refresh_duckdb(conn)

    return conn


def refresh_duckdb(conn: duckdb.DuckDBPyConnection):
    conn.execute("DROP SCHEMA IF EXISTS dwh CASCADE")

    path = os.path.dirname(__file__)
    files = os.listdir(path + "/duckdb")
    files.sort()
    for file in files:
        sql = open(path + "/duckdb/" + file).read()
        conn.execute(sql)
