from datetime import datetime
import pyodbc
from typing import Callable


from core import Core, EnvDistributor, tprint


class DatabaseBroker:
    """
    Container to hold all related data of a IBAPI contract instance.
    Written for T-SQL.
    """

    def __init__(self):

        self.core: Core = EnvDistributor.get_core()

        connection_string: str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.core.SQL_SERVER};UID={self.core.SQL_USER};PWD={self.core.SQL_PASSWORD}'
        self.connection_string = connection_string
        self.table_structure = {}

        self.sql_ignore = ['master', 'tempdb', 'model', 'msdb']

        pass

    def sql_query(func) -> Callable:
        def con_wrapper(self, *args, **kwargs) -> dict | None:
            sql_con: pyodbc.Connection = pyodbc.connect(self.connection_string)
            cursor: pyodbc.Cursor = sql_con.cursor()

            # tprint(f'Con opened for {func.__name__}', Debug=True)
            # tprint(f'{args=}', Debug=True)
            # tprint(f'{kwargs=}', Debug=True)

            result = func(self, cursor=cursor, con=sql_con, *args, **kwargs) or {}

            if isinstance(result, dict) and 'commit' in result.keys():
                sql_con.commit()

            cursor.close()
            sql_con.close()

            return result['data'] if isinstance(result, dict) and 'commit' in result.keys() else None

        return con_wrapper

    def get_all_table_names(self, cursor: pyodbc.Cursor, db_name: str = None, return_data: bool = False, **kwargs) ->\
            dict[str: None | dict[str, list[str]], str: bool]:
        """
            Fetches the names of all tables in the database.

            This method queries the database for the names of all tables in all databases,
            excluding the databases listed in `self.sql_ignore`. The results are returned
            as a dictionary where the keys are the database names and the values are lists
            of table names.

            Args:
                cursor (object): The database cursor. [Provided by wrapper]
                db_name (str, optional): The name of the database. Defaults to None.

            Returns:
                dict: A dictionary containing not data a flag indicating if the operation needs to be committed.
            """

        query = 'SELECT name FROM sys.databases'
        cursor.execute(query)
        if db_name is None:
            database_names = [x[0] for x in cursor.fetchall() if x[0] not in self.sql_ignore]
        else:
            database_names = [db_name]

        for db in database_names:
            query = f'USE [{db}]'
            cursor.execute(query)
            query = f'SELECT name FROM sys.tables'
            cursor.execute(query)

            self.table_structure[db] = [x[0] for x in cursor.fetchall()]

        if return_data:
            return {'data': self.table_structure, 'commit': False}
        else:
            return {'data': None, 'commit': False}

    @sql_query
    def check_table_exists(self, cursor: pyodbc.Cursor, contract_container: 'ContractContainer', create_missing: bool = True, **kwargs) -> dict[str: None, str: bool]:
        """
         Check if a database and a table exists for a given contract.

         Args:
             cursor (pyodbc.Cursor): The database cursor. [Provided by wrapper]
             contract_container (ContractContainer): The contract object.
             create_missing (bool, optional): Whether to create the table if it doesn't exist. Defaults to True.

         Returns:
             dict: A dictionary containing the latest update and a flag indicating if the operation needs to be committed.
         """

        if self.table_structure == {}:
            self.get_all_table_names()

        contract = contract_container.get_contract()
        match contract.secType:
            case 'STK':
                database_name = 'Data_STK'
                table_name = f'{contract.symbol.replace('.', '')}_STK'
            case 'OPT':
                database_name = f'Data_OPT_{datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%b%y')}'
                table_name = f'{contract.symbol.replace('.', '')}_OPT_{datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%d%b%y')}'
            case _:
                raise KeyError(f'Security type {contract.secType} not supported.')

        if create_missing:
            if database_name not in self.table_structure.keys():
                tprint(f'New database created: {database_name}')
                self.create_database(db_name=database_name)
                self.get_all_table_names(database=database_name)

            if table_name not in self.table_structure[database_name]:
                tprint(f'New table created: {table_name}.')
                self.create_table(database=database_name, table=table_name)
                self.get_all_table_names(database=database_name)

        return {'data': None, 'commit': False}

    @sql_query
    def get_last_update(self, cursor: pyodbc.Cursor, contract: 'ContractContainer', **kwargs) -> dict[str: datetime, str: bool]:
        """
        Fetches the latest update from the database for a given contract.

        Args:
            cursor (pyodbc.Cursor): The database cursor.
            contract_container (ContractContainer): The contract object.

        Raises:
            TypeError: If the contract is not an instance of the Contract class.
            KeyError: If the security type is not supported.

        Returns:
            dict: A dictionary containing the latest update and a flag indicating if the operation was committed.
        """

        contract = contract_container.get_contract()
        database = contract_container.get_database()
        table = contract_container.get_table()
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

        while True:
            try:
                cursor.execute(query)
                break
            except pyodbc.ProgrammingError as e:
                self.check_table_exists(contract_container=contract_container, create_missing=True)

        last_update = cursor.fetchone()
        last_update = last_update[0] if last_update is not None else None

        return {'data': last_update, 'commit': False}

    @sql_query
    def get_last_price(self, cursor: pyodbc.Cursor, stk_symbol: str, **kwargs) -> dict[str: float | None:, str: bool ] :
        query = f"""
                SELECT c
                FROM [Data_STK].[dbo].[{f'{stk_symbol}_STK'}]
                WHERE date = (
                    SELECT max(date)
                    FROM [Data_STK].[dbo].[{f'{stk_symbol}_STK'}]
                    );
                """
        cursor.execute(query)

        last_price = cursor.fetchone()
        last_price = last_price[0] if last_price is not None else None

        return {'data': last_price, 'commit': False}

    @sql_query
    def create_database(self, cursor: pyodbc.Cursor, con: pyodbc.Connection, db_name: str, **kwargs):

        con.autocommit = True
        query = f'CREATE DATABASE {db_name}'
        cursor.execute(query)
        con.autocommit = False

        return {'data': True, 'commit': True}

    @sql_query
    def create_table(self, cursor: pyodbc.Cursor, db_name: str, table_name: str, **kwargs) -> dict[str: None, str: bool]:

        match table_name.count('_'):
            case 1: # STK
                query = f'USE [{db_name}]'
                cursor.execute(query)

                table_name = f'{table_name.replace('.', '')}'
                query = """CREATE TABLE {table_name} (
                                    date DATETIME,
                                    h FLOAT,
                                    l FLOAT,
                                    o FLOAT,
                                    c FLOAT);
                            """.format(table_name=table_name)
            case 2: # OPT
                query = f'USE [{db_name}]'
                cursor.execute(query)
                query = """CREATE TABLE {table_name} (
                                    date DATETIME,
                                    identifier VARCHAR(50),
                                    callput VARCHAR(1),
                                    strike FLOAT,
                                    h FLOAT,
                                    l FLOAT,
                                    o FLOAT,
                                    c FLOAT);
                            """.format(table_name=table_name)
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')
        #print('Create table query: ', query)
        cursor.execute(query)

        return {'data': None, 'commit': True}

    @sql_query
    def write_price_data(self, cursor, query_string: str, **kwargs) -> dict[str: None, str: bool]:
        if query_string:
            cursor.execute(query_string)
            return {'data': None, 'commit': True}
        raise Exception('No query string provided.')

    @sql_query
    def get_existing_dates(self, cursor, contract_container: 'ContractContainer' = None, **kwargs) -> dict[str: set[datetime], str: bool]:

        contract = contract_container.get_contract()
        database = contract_container.get_database()
        table = contract_container.get_table()

        match contract.secType:
            case 'STK':
                query = f"""
                    SELECT DISTINCT date
                    FROM [{database}].[dbo].[{table}]
                    ORDER BY date DESC;
                    """
            case 'OPT':
                query = f"""
                    SELECT DISTINCT date
                    FROM [{database}].[dbo].[{table}]
                    WHERE strike = {contract.strike}
                    AND callput = '{contract.right}';
                    """
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

        cursor.execute(query)
        existing_dates = set(x[0] for x in cursor.fetchall())

        return {'data': existing_dates, 'commit': False}

    @sql_query
    def split_table(self, cursor, db_name: str = None, **kwargs):
        if db_name is None:
            raise ValueError('Database name must be provided.')
        query = f'SELECT * FROM [{db_name}]'
        cursor.execute(query)

    # @staticmethod
        # def check_contract_type(contract):
        #     if not isinstance(contract, "ContractContainer"):
        #         raise TypeError('Contract must be an instance of the contract class.')


