from abc import ABC, abstractmethod
from datetime import datetime

import psycopg2
from psycopg2 import extensions
from psycopg2 import errors
import pyodbc

from core import EnvDistributor


class SQL(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_databases(self):
        pass

    @abstractmethod
    def get_tables(self, database: str):
        pass

    @abstractmethod
    def create_database(self, database: str):
        pass

    # @abstractmethod
    # def toogle_autocommit(self, toggle: bool):
    #     pass

    @abstractmethod
    def create_table(self, database: str, table: str, query: str = None):
        pass

    @abstractmethod
    def switch_database(self, database: str):
        pass

    @abstractmethod
    def get_last_entry(self, contract_container: 'ContractContainer', database: str, table: str) -> datetime | None:
        pass

    @abstractmethod
    def get_last_price(self, symbol: str) -> float | None:
        pass

    @abstractmethod
    def write_price_data(self, query_string: str, database: str):
        pass

    @abstractmethod
    def generate_insert_header(self, database: str, table:str , columns: list[str]) -> str:
        pass

    @abstractmethod
    def get_existing_dates(self, database: str, table: str, sec_type: str, strike: float, right: str) -> set[datetime]:
        pass


class MSSQL(SQL):
    def __init__(self):
        self.core = EnvDistributor.get_core()
        self.sql_ignore = ['master', 'tempdb', 'model', 'msdb']

        if self.core.SQL_PORT == '0':
            self.core.SQL_PORT = '1433'

        self.con, self.cursor = None, None

        self.active_database = None

        self.connect()

    def connect(self):
        connection_string: str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.core.SQL_SERVER},{self.core.SQL_PORT};'
            f'UID={self.core.SQL_USER};'
            f'PWD={self.core.SQL_PASSWORD}'
        )
        self.con: pyodbc.Connection = pyodbc.connect(connection_string)
        self.cursor: pyodbc.Cursor = self.con.cursor()

    def get_databases(self) -> list[str]:
        query = 'SELECT name FROM sys.databases'
        self.cursor.execute(query)

        result = [x[0] for x in self.cursor.fetchall() if x[0] not in self.sql_ignore]

        return result

    def switch_database(self, database: str):
        if database != self.active_database:
            query = f'USE [{database}]'
            self.cursor.execute(query)
            self.active_database = database

    def get_tables(self, database: str) -> list[str]:
        self.switch_database(database=database)
        query = f'SELECT name FROM sys.tables'
        self.cursor.execute(query)

        result = [x[0] for x in self.cursor.fetchall()]

        return result

    def create_database(self, database: str):

        self.toggle_autocommit(True)
        query = f'CREATE DATABASE {database}'
        self.cursor.execute(query)
        self.toggle_autocommit(False)

    def toggle_autocommit(self, toggle: bool = False):
        if toggle:
            self.con.autocommit = True
        else:
            self.con.autocommit = False

    def create_table(self, database: str, table: str, query: str = None):
        table = table.replace('.', '')

        if not query:
            match table.split('_')[1].lower():
                case 'stk':
                    query = f"""CREATE TABLE {table} (
                                date DATETIME,
                                h FLOAT,
                                l FLOAT,
                                o FLOAT,
                                c FLOAT);
                                """
                case 'opt':
                    query = f"""CREATE TABLE {table} (
                            date DATETIME,
                            identifier VARCHAR(50),
                            callput VARCHAR(1),
                            strike FLOAT,
                            h FLOAT,
                            l FLOAT,
                            o FLOAT,
                            c FLOAT);
                            """
                case _:
                    raise Exception(f'No table format defined for {table} secType. Valid secTypes: STK, OPT')

        self.switch_database(database=database)
        self.toggle_autocommit(True)
        self.cursor.execute(query)
        self.toggle_autocommit(False)

    def get_last_entry(self, contract: 'contract', database: str, table: str) -> datetime | None:
        table = table.replace('.', '')

        match contract.secType:
            case 'STK':
                query = f"""
                        SELECT MAX(date)
                        FROM [{database}].[dbo].[{table}]
                        """
            case 'OPT':
                query = f"""
                        SELECT MAX(date)
                        FROM [{database}].[dbo].[{table}]
                        WHERE strike = {contract.strike}
                        AND callput = '{contract.right}';
                        """
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

        self.cursor.execute(query)

        last_update = self.cursor.fetchone()
        result = last_update[0] if last_update is not None else None

        return result

    def get_last_price(self, symbol: str) -> float | None:
        table =  f'{symbol.replace('.', '')}_STK'

        query = f"""
                SELECT c
                FROM [Data_STK].[dbo].[{table}]
                WHERE date = (
                    SELECT max(date)
                    FROM [Data_STK].[dbo].[{table}]
                    );
                """

        self.cursor.execute(query)

        last_price = self.cursor.fetchone()
        result = last_price[0] if last_price is not None else None

        return result

    def generate_insert_header(self, database: str, table: str, columns: list[str]) -> str:
        header= f"""
                INSERT INTO [{database}].[dbo].[{table}] ({columns})
                VALUES
                """

        return header

    def write_price_data(self, insert_query: str, database: str):
        self.cursor.execute(insert_query)

    def get_existing_dates(self, database: str, table: str, sec_type: str, strike: float = None, right: str = None) -> set[datetime]:
        match sec_type.lower():
            case 'stk':
                query = f"""
                        SELECT DISTINCT date
                        FROM [{database}].[dbo].[{table}]
                        ORDER BY date DESC;
                        """
            case 'opt':
                query = f"""
                        SELECT DISTINCT date
                        FROM [{database}].[dbo].[{table}]
                        WHERE strike = {strike}
                        AND callput = '{right}';
                        """
            case _:
                raise Exception('Security type not supported. Valid secTypes: STK, OPT')

        self.cursor.execute(query)
        result = set(x[0] for x in self.cursor.fetchall())

        return result

class PGSQL(SQL):
    def __init__(self):
        self.core = EnvDistributor.get_core()

        if self.core.SQL_PORT == '0':
            self.core.SQL_PORT = '5432'

        self.DEFAULT_DATABASE = 'market_data'

        self.con, self.cursor = None, None
        self.active_database = None
        self.active_schema = None
        self.available_schemas = {}

        self.connect()

    def connect(self, database: str = None) -> None:
        if not database:
            database = self.DEFAULT_DATABASE

        self.con: extensions.connection = psycopg2.connect(
            host=self.core.SQL_SERVER,
            port=self.core.SQL_PORT,
            dbname=database,
            user=self.core.SQL_USER,
            password=self.core.SQL_PASSWORD)

        self.cursor: extensions.cursor = self.con.cursor()

        self.active_database = database
        self.active_schema = None

    def switch_database(self, database: str):
        database = database.lower()
        if database != self.active_database:
            self.connect(database=database)

    def switch_schema(self, schema: str):
        schema = schema.lower()
        if schema != self.active_schema:
            self.check_schema_exists(schema=schema)
            self.cursor.execute(f'SET search_path TO {schema}')
            #print(f'Schema switched to {schema}')
            self.active_schema = schema

    def get_schemas(self) -> list[str]:
        query = """ SELECT schema_name
                    FROM information_schema.schemata
                    ORDER BY schema_name;
                """

        self.cursor.execute(query)
        result = [x[0] for x in self.cursor.fetchall()]
        self.available_schemas[self.active_database] = result

        return result

    def check_schema_exists(self, schema: str, create: bool = True):
        if not self.available_schemas:
            self.get_schemas()

        if schema not in self.available_schemas.get(self.active_database, []) and create:
            query = f'CREATE SCHEMA {schema}'

            self.cursor.execute(query)
            self.con.commit()

    def get_databases(self) -> list[str]:
        return self.get_schemas()

    def get_tables(self, database: str) -> list[str]:

        #self.switch_schema(schema=database)

        query = f"""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{database}' AND table_type = 'BASE TABLE';
                """

        self.cursor.execute(query)

        result = [x[0] for x in self.cursor.fetchall()]

        return result

    def create_database(self, database: str):

        query = f'CREATE DATABASE {database.lower()}'
        self.cursor.execute(query)
        self.con.commit()

    def toggle_autocommit(self, toggle: bool = False):
        if toggle:
            self.con.autocommit = True
        else:
            self.con.autocommit = False

    def create_table(self, database: str, table: str, query: str = None):
        table = table.lower().replace('.', '')

        if not query:
            match table.split('_')[1].lower():
                case 'stk':
                    query = f"""CREATE TABLE {database.lower()}.{table} (
                            date TIMESTAMP,
                            h DOUBLE PRECISION,
                            l DOUBLE PRECISION,
                            o DOUBLE PRECISION,
                            c DOUBLE PRECISION);
                            """

                case 'opt':
                    query = f"""CREATE TABLE {database.lower()}.{table} (
                                date TIMESTAMP,
                                identifier VARCHAR(50),
                                callput VARCHAR(1),
                                strike DOUBLE PRECISION,
                                h DOUBLE PRECISION,
                                l DOUBLE PRECISION,
                                o DOUBLE PRECISION,
                                c DOUBLE PRECISION);
                                """
                case _:
                    raise Exception(f'No table format defined for {table} secType. Valid secTypes: STK, OPT')

        #self.switch_schema(schema=database)
        self.cursor.execute(query)
        self.con.commit()

    def get_last_entry(self, contract: 'contract', database: str, table: str) -> datetime | None:
        #self.switch_schema(schema=database)
        table = table.lower().replace('.', '')

        match contract.secType:
            case 'STK':
                query = f"""
                        SELECT MAX(date)
                        FROM {database.lower()}.{table}
                        """
            case 'OPT':
                query = f"""
                        SELECT MAX(date)
                        FROM {database.lower()}.{table}
                        WHERE strike = {contract.strike}
                        AND callput = '{contract.right}';
                        """
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

        self.cursor.execute(query)

        try:
            last_update = self.cursor.fetchone()
        except psycopg2.ProgrammingError:
            print('Programming Error.')
            print(f'{self.active_database}')
            print(f'{query=}')
            return None
        except errors.InFailedSqlTransaction as e:
            print('Error InFailedSqlTransaction.')
            print(query)
            print(f'Schema {database} {self.active_database}')
            print('Need to create table?')
            return None

        return last_update[0] if last_update is not None else None

    def get_last_price(self, symbol: str) -> float | None:
        #self.switch_schema(schema='data_stk')
        table =  f'{symbol.lower().replace('.', '')}_stk'

        query = f"""
                SELECT c
                FROM data_stk.{table}
                WHERE date = (
                    SELECT max(date)
                    FROM data_stk.{table}
                    );
                """

        self.cursor.execute(query)

        last_price = self.cursor.fetchone()
        result = last_price[0] if last_price is not None else None

        return result

    def generate_insert_header(self, database: str, table: str, columns: list[str]) -> str:
        header = f"""
                INSERT INTO {database.lower()}.{table} ({columns})
                VALUES
                """

        return header

    def write_price_data(self, insert_query: str, database: str):
        #self.switch_schema(schema=database)
        self.cursor.execute(insert_query)
        self.con.commit()

    def get_existing_dates(self, database: str, table: str, sec_type: str, strike: float = None, right: str = None) -> \
    set[datetime]:
        #self.switch_schema(schema=database)

        match sec_type.lower():
            case 'stk':
                query = f"""
                        SELECT DISTINCT date
                        FROM {database.lower()}.{table}
                        ORDER BY date DESC;
                        """
            case 'opt':
                query = f"""
                        SELECT DISTINCT date
                        FROM {database.lower()}.{table}
                        WHERE strike = {strike}
                        AND callput = '{right}'
                        ORDER BY date DESC;
                        """
            case _:
                raise Exception('Security type not supported. Valid secTypes: STK, OPT')

        self.cursor.execute(query)
        result = set(x[0] for x in self.cursor.fetchall())

        return result





