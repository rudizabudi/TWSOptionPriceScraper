import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import pyodbc
import requests

from core import tprint

load_dotenv('.env')

# Microsoft SQL Server Credentials
sql_server: str = os.getenv('SQL_SERVER')
sql_user: str = os.getenv('SQL_USER')
sql_password: str = os.getenv('SQL_PASSWORD')


def list_updater(core):
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={core.sql_server};UID={core.sql_user};PWD={core.sql_password}'
    sql_conn: pyodbc.Connection = pyodbc.connect(connection_string)
    cursor = sql_conn.cursor()

    resp = requests.get(core.update_csv_path)
    current_symbols = []
    for row in resp.text.split('\n')[1:-1]:
        current_symbols.append(row.split(',')[0])

    create_table = True
    tb = 'Query_Constituents'
    if create_table:
        try:
            query = f"""CREATE TABLE {tb} (
                        symbol VARCHAR(100) PRIMARY KEY,
                        last_seen DATETIME,
                        always_update BIT DEFAULT 0);
                    """
            cursor.execute(query)
            sql_conn.commit()
        except pyodbc.ProgrammingError:
            tprint(f'Table {tb} already exists.')

    db = 'Data_STK'
    query = f'SELECT symbol FROM [{db}].[dbo].[{tb}]'
    cursor.execute(query)
    old_entries = set(x[0] for x in cursor.fetchall())

    new_counter = 0
    columns = 'symbol, last_seen, always_update'
    for symbol in current_symbols:
        if symbol in old_entries:
            query = f"""
                    UPDATE [{db}].[dbo].[{tb}]
                    SET last_seen = '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}'
                    WHERE symbol = '{symbol}';
                    """
        else:
            query = f"""
                    INSERT INTO [{db}].[dbo].[{tb}] ({columns})
                    VALUES ('{symbol}', '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}', 0)
                    """
            new_counter += 1
        cursor.execute(query)

    for symbol in core.extra_symbols:
        if symbol not in old_entries:
            query = f"""
                    INSERT INTO [{db}].[dbo].[{tb}] ({columns})
                    VALUES ('{symbol}', NULL, 1)
                    """
            new_counter += 1
            cursor.execute(query)

    sql_conn.commit()
    if new_counter:
        tprint('{new_counter} new underlyings added to S&P constituents list.')

    query = f'SELECT * FROM [{db}].[dbo].[{tb}]'
    cursor.execute(query)
    response = cursor.fetchall()

    query_list = []
    for symbol, last_seen, always_update in response:
        if always_update:
            query_list.append(symbol)
        elif datetime.today() <= last_seen + timedelta(days=core.grace_period):
            query_list.append(symbol)

    core.underlying_list['STK'].extend(query_list)
    cursor.close()
    sql_conn.close()
    
    tprint(f'Underlying list updated.')
