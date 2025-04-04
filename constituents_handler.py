from dataclasses import dataclass
from datetime import datetime, timedelta
import pyodbc
import requests

from core import tprint


@dataclass
class ListUpdaterSettings:
    constituents_db: str = 'Data_STK'
    constituents_table: str = 'Query_Constituents'


def constituents_list_updater(core):
    sql_conn: pyodbc.Connection = pyodbc.connect(core.SQL_CONNECTION_STRING)
    cursor = sql_conn.cursor()

    resp = requests.get(core.UPDATE_CSV_PATH)
    current_symbols = []
    for row in resp.text.split('\n')[1:-1]:
        current_symbols.append(row.split(',')[0])

    cursor.execute(f'USE [{ListUpdaterSettings.constituents_db}]')

    query = (f"""
             SELECT name FROM sys.tables
             """)
    cursor.execute(query)

    table_names = set(x[0] for x in cursor.fetchall() if x[0])

    if ListUpdaterSettings.constituents_table not in table_names:
        query = f"""CREATE TABLE {ListUpdaterSettings.constituents_table} (
                        symbol VARCHAR(100) PRIMARY KEY,
                        last_seen DATETIME,
                        always_update BIT DEFAULT 0);
                    """
        cursor.execute(query)
        sql_conn.commit()

    query = f'SELECT symbol FROM [{ListUpdaterSettings.constituents_db}].[dbo].[{ListUpdaterSettings.constituents_table}]'
    cursor.execute(query)
    old_entries = set(x[0] for x in cursor.fetchall())

    new_counter = 0
    columns = 'symbol, last_seen, always_update'
    for symbol in current_symbols:
        if symbol in old_entries:
            query = f"""
                    UPDATE [{ListUpdaterSettings.constituents_db}].[dbo].[{ListUpdaterSettings.constituents_table}]
                    SET last_seen = '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}'
                    WHERE symbol = '{symbol}';
                    """
        else:
            query = f"""
                    INSERT INTO [{ListUpdaterSettings.constituents_db}].[dbo].[{ListUpdaterSettings.constituents_table}] ({columns})
                    VALUES ('{symbol}', '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}', 0)
                    """
            new_counter += 1
        cursor.execute(query)

    for extra_symbol in core.EXTRA_SYMBOLS:
        if extra_symbol not in old_entries:
            query = f"""
                    INSERT INTO [{ListUpdaterSettings.constituents_db}].[dbo].[{ListUpdaterSettings.constituents_table}] ({columns})
                    VALUES ('{extra_symbol}', NULL, 1)
                    """
            new_counter += 1
            cursor.execute(query)

    sql_conn.commit()

    if new_counter:
        tprint('{new_counter} new underlyings added to S&P constituents list.')
    else:
        tprint('Constituents list is up to date.')

    cursor.close()
    sql_conn.close()


def load_constituents(core):
    sql_conn: pyodbc.Connection = pyodbc.connect(core.SQL_CONNECTION_STRING)
    cursor = sql_conn.cursor()

    query = f'SELECT * FROM [{ListUpdaterSettings.constituents_db}].[dbo].[{ListUpdaterSettings.constituents_table}]'
    cursor.execute(query)

    scraper_input_list = cursor.fetchall()

    temp_underlying_list = []
    for symbol, last_seen, always_update in scraper_input_list:
        if always_update:
            temp_underlying_list.append(symbol)
        elif datetime.today() <= last_seen + timedelta(days=core.GRACE_PERIOD):
            temp_underlying_list.append(symbol)

    core.underlying_list['STK'].extend(temp_underlying_list)

    tprint('Constituents loaded.')

    cursor.close()
    sql_conn.close()

