import pickle
from datetime import datetime #TODO: Check if double imports across files are best practice

import pyodbc
from ibapi.contract import Contract


class DatabaseBroker():
    """
    Container to hold all related data of a IBAPI contract instance.
    Written for T-SQL.
    """
    def __init__(self, core=None, CC=None):
        if None in (core, CC):
            raise Exception('<DatabaseBroker INIT> All parameters must be specified.')
        self.connection_string = core.connection_string

        self.table_structure = {}

        self.sql_ignore = ['master', 'tempdb', 'model', 'msdb']

        self.ContractContainer = CC
        pass

    def sql_query(func) -> object:
        def con_wrapper(self, *args, **kwargs):
            sql_con: pyodbc.Connection = pyodbc.connect(self.connection_string)
            #print(f'Con opened for {func.__name__}')

            cursor: pyodbc.Cursor = sql_con.cursor()
            #print('Cursor opened')

            result = func(self, cursor = cursor, conn = sql_con, *args, **kwargs) or {}

            if isinstance(result, dict) and 'commit' in result.keys():
                sql_con.commit()

            cursor.close()
            #print('Cursor closed')
            sql_con.close()
            #print('Con closed')

            return result['data'] if isinstance(result, dict) and 'commit' in result.keys() else None

        return con_wrapper

    @sql_query
    def fetch_all_table_names(self, cursor: pyodbc.Cursor, db_name: str = None, **kwargs) -> dict:
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

        return {'data': [], 'commit': False}

    @sql_query
    def check_table_exists(self, cursor: pyodbc.Cursor, contract_container: "ContractContainer", create_missing: bool = True, **kwargs):
        """
         Check if a database and a table exists for a given contract.

         Args:
             cursor (pyodbc.Cursor): The database cursor. [Provided by wrapper]
             contract (Contract): The contract object.
             create_missing (bool, optional): Whether to create the table if it doesn't exist. Defaults to True.

         Returns:
             dict: A dictionary containing the latest update and a flag indicating if the operation needs to be committed.
         """


        if self.table_structure == {}:
            self.fetch_all_table_names()

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
                self.create_database(db_name=database_name)
                self.fetch_all_table_names(database=database_name)

            if table_name not in self.table_structure[database_name]:
                self.create_table(db_name=database_name, table_name=table_name)
                self.fetch_all_table_names(database=database_name)

        return {'data': None, 'commit': False}

    @sql_query
    def get_last_update(self, cursor: pyodbc.Cursor, contract_container: "ContractContainer" = None, **kwargs) -> dict:
        """
        Fetches the latest update from the database for a given contract.

        Args:
            cursor (pyodbc.Cursor): The database cursor.
            contract (object, optional): The contract object. Defaults to None.

        Raises:
            TypeError: If the contract is not an instance of the Contract class.
            KeyError: If the security type is not supported.

        Returns:
            dict: A dictionary containing the latest update and a flag indicating if the operation was committed.
        """

        contract = self.ContractContainer.get_contract()
        database = self.ContractContainer.get_database()
        match contract.secType:
            case 'STK':
                table_name = f'{contract.symbol}_STK'
                query = f"""
                        SELECT MAX(date)
                        FROM [{database}].[dbo].[{table_name}]
                        """
            case 'OPT':
                expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%d%b%y')
                table_name = f'{contract.symbol}_OPT_{expiry_date}'
                query = f"""
                        SELECT MAX(date)
                        FROM [{database}].[dbo].[{table_name}]
                        WHERE strike = {contract.strike}
                        AND callput = '{contract.right}';
                        """
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

        cursor.execute(query)
        last_update = cursor.fetchone()
        last_update = last_update[0] if last_update is not None else None

        return {'data': last_update, 'commit': False}

    @sql_query
    def create_database(self, cursor: pyodbc.Cursor, conn: pyodbc.Connection, db_name: str, **kwargs):

        conn.autocommit = True
        query = f'CREATE DATABASE {db_name}'
        cursor.execute(query)
        conn.autocommit = False

        return {'data': True, 'commit': True}


    @sql_query
    def create_table(self, cursor: pyodbc.Cursor, db_name: str, table_name: str, **kwargs) -> dict:

        match table_name.count('_'):
            case 1: # STK
                query = f'USE [{db_name}]'
                cursor.execute(query)

                table_name = f'{table_name.replace('.', '')}_STK'
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
        #cursor.execute(query)

        return {'data': True, 'commit': True}

    @sql_query
    def insert_data(self, cursor, contract_container: "ContractContainer" = None, **kwargs) -> dict: #TODO: FINISH!

        contract = contract_container.get_contract()

        self.check_contract_type(contract)

        match contract.secType:
            case 'STK':
                table_name = f'{contract.symbol.replace(".", "")}_STK'
                query = f"""INSERT INTO [{self.database}].[dbo].[{table_name}]
                            VALUES (?, ?, ?, ?, ?)
                        """
            case 'OPT':
                expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%d%b%y')
                table_name = f'{contract.symbol}_OPT_{expiry_date}'
                query = f"""
                    INSERT INTO [{self.database}].[dbo].[{table_name}]""" # T

    @sql_query
    def get_existing_dates(self, cursor, contract_container: "ContractContainer" = None, **kwargs) -> dict:

        contract = contract_container.get_contract()

        match contract.secType:
            case 'STK':
                table_name = f'{contract.symbol.replace(".", "")}_STK'
                query = f"""
                    SELECT DISTINCT date
                    FROM [{self.database}].[dbo].[{table_name}]
                """
            case 'OPT':
                expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%d%b%y')
                table_name = f'{contract.symbol}_OPT_{expiry_date}'
                query = f"""
                    SELECT DISTINCT date
                    FROM [{self.database}].[dbo].[{table_name}]
                """
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

        cursor.execute(query)
        existing_dates = [x[0] for x in cursor.fetchall()] # List because of dynamic cursor

        return {'data': existing_dates, 'commit': False}

    @staticmethod
    def check_contract_type(contract):
        if not isinstance(contract, ContractContainer):
            raise TypeError('Contract must be an instance of the contract class.')


