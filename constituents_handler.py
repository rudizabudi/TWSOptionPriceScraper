from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pyodbc
import requests

from core import tprint, EnvDistributor, Core
from database_broker import DatabaseBroker
from sql import MSSQL, PGSQL


@dataclass
class ListUpdaterSettingsMSSQL:
    constituents_db: str = 'Data_STK'
    constituents_table: str = 'Query_Constituents'

@dataclass
class ListUpdaterSettingsPGSQL:
    constituents_schema: str = 'utils'
    constituents_table: str = 'query_constituents'


def constituents_list_updater(core):
    sql = SQL(core).sql

    resp = requests.get(core.UPDATE_CSV_PATH)
    current_symbols = []
    for row in resp.text.split('\n')[1:-1]:
        current_symbols.append(row.split(',')[0])

    sql.check_structure()

    old_entries = sql.get_symbols()

    new_counter = 0
    for symbol in current_symbols:
        if symbol in old_entries:
            sql.update_last_seen(symbol)
        else:
            sql.first_seen(symbol)
            new_counter += 1

    for extra_symbol in core.EXTRA_SYMBOLS:
        if extra_symbol not in old_entries:
            sql.add_extra_symbol(extra_symbol)
            new_counter += 1

    if new_counter:
        tprint(f'{new_counter} new underlyings added to S&P constituents list.')
    else:
        tprint('Constituents list is up to date.')

    sql.close()


def load_constituents(core):
    sql = SQL(core).sql

    scraper_input_list = sql.get_table()

    temp_underlying_list = []
    for symbol, last_seen, always_update, data in scraper_input_list:
        if always_update:
            temp_underlying_list.append(symbol)
        elif datetime.today() <= last_seen + timedelta(days=core.GRACE_PERIOD):
            temp_underlying_list.append(symbol)

    core.underlying_list['STK'].extend(temp_underlying_list)

    sql.close()
    tprint('Constituents loaded.')


class ConstituentsMSQL(MSSQL):
    def __init__(self, columns):
        super().__init__()
        self.columns = columns
        self.constituents_db = ListUpdaterSettingsMSSQL.constituents_db
        self.constituents_table = ListUpdaterSettingsMSSQL.constituents_table

    def check_structure(self):
        if self.constituents_table not in self.get_databases():
            self.create_database(database=self.constituents_db)

        if self.constituents_table not in self.get_tables(database=self.constituents_db):
            query = f"""CREATE TABLE {self.constituents_table} (
                            symbol VARCHAR(100) PRIMARY KEY,
                            last_seen DATETIME,
                            always_update BIT DEFAULT 0);
                    """
            self.create_table(database=self.constituents_db,
                              table=self.constituents_table,
                              query=query)

    def get_symbols(self) -> set[str]:
        query = f'SELECT symbol FROM [{self.constituents_db}].[dbo].[{self.constituents_table}]'
        self.cursor.execute(query)
        return set(x[0] for x in self.cursor.fetchall())

    def update_last_seen(self, symbol: str):
        query = f"""
                UPDATE [{self.constituents_db}].[dbo].[{self.constituents_table}]
                SET last_seen = '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}'
                WHERE symbol = '{symbol}';
                """
        self.cursor.execute(query)

    def first_seen(self, symbol: str):
        query = f"""
                INSERT INTO [{self.constituents_db}].[dbo].[{self.constituents_table}] ({self.columns})
                VALUES ('{symbol}', '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}', 0)
                """
        self.cursor.execute(query)

    def add_extra_symbol(self, symbol: str):
        query = f"""
                INSERT INTO [{self.constituents_db}].[dbo].[{self.constituents_table}] ({self.columns})
                VALUES ('{symbol}', NULL, 1)
                """
        self.cursor.execute(query)

    def get_table(self) -> list[Any]:
        query = f'SELECT * FROM [{self.constituents_db}].[dbo].[{self.constituents_table}]'
        self.cursor.execute(query)

        return self.cursor.fetchall()

    def close(self):
        self.cursor.commit()
        self.cursor.close()
        self.con.close()

class ConstituentsPGSQL(PGSQL):
    def __init__(self, columns):
        super().__init__()
        self.columns = columns
        self.constituents_schema = ListUpdaterSettingsPGSQL.constituents_schema
        self.constituents_table = ListUpdaterSettingsPGSQL.constituents_table

    def check_structure(self):
        self.check_schema_exists(schema=self.constituents_schema)

        if self.constituents_table not in self.get_tables(database=self.constituents_schema):
            query = f"""CREATE TABLE {self.constituents_table} (
                        symbol TEXT PRIMARY KEY,
                        last_seen TIMESTAMP,
                        always_update BOOLEAN DEFAULT FALSE);
                    """

            self.create_table(database=self.constituents_schema,
                              table=self.constituents_table,
                              query=query)

    def get_symbols(self) -> set[str]:
        self.switch_schema(schema=self.constituents_schema)
        query = f'SELECT symbol FROM {self.constituents_table}'
        self.cursor.execute(query)
        return set(x[0] for x in self.cursor.fetchall())

    def update_last_seen(self, symbol: str):
        self.switch_schema(schema=self.constituents_schema)

        query = f"""
                UPDATE {self.constituents_table}
                SET last_seen = '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}'
                WHERE symbol = '{symbol}';
                """
        self.cursor.execute(query)

    def first_seen(self, symbol: str):
        self.switch_schema(schema=self.constituents_schema)

        query = f"""
                INSERT INTO {self.constituents_table} ({self.columns})
                VALUES ('{symbol}', '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}', 0)
                """
        self.cursor.execute(query)

    def add_extra_symbol(self, symbol: str):
        self.switch_schema(schema=self.constituents_schema)

        query = f"""
                INSERT INTO {self.constituents_table} ({self.columns})
                VALUES ('{symbol}', NULL, 1)
                """
        self.cursor.execute(query)

    def get_table(self) -> list[Any]:
        self.switch_schema(schema=self.constituents_schema)

        query = f'SELECT * FROM {self.constituents_table}'
        self.cursor.execute(query)

        return self.cursor.fetchall()

    def close(self):
        self.con.commit()
        self.cursor.close()
        self.con.close()

class SQL:
    def __init__(self, core):
        self.core: Core = core
        self.columns = 'symbol, last_seen, always_update'

        match self.core.SQL_TYPE:
            case 'MSSQL':
                self.sql = ConstituentsMSQL(self.columns)
            case 'PGSQL':
                self.sql = ConstituentsPGSQL(self.columns)
            case _:
                raise Exception('Provided SQL Type is not supported.')