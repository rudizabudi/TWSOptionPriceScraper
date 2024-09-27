from datetime import datetime, time, timedelta
from dotenv import load_dotenv, set_key
import pickle
import pyodbc
import random
from time import sleep
from threading import Thread

from contract_container import ContractContainer
from core import tprint


class PipelineBuilder:
    def __init__(self, core =None, tws_con=None, CC=None, DB=None):
        if None in (core, tws_con, CC, DB):
            raise Exception('<PipelineBuilder INIT> All parameters must be specified.')

        self.core = core
        self.tws_con = tws_con
        self.ContractContainer = CC

        self.db = DB(core=self.core, CC=self.ContractContainer)

        self.stk_sorter_pointer: int = 0

        self.t1 = Thread(target=self.pipeline_sorter, daemon=True).start()

        self.debug_load = False

        self.option_exp_max_length = 0

    def startup_build_sequence(self):
        """
        Coordinates startup build of all self.core.contract_pool types (STK, OPT, EXP)

        If finished sets flag for main purpose threads to start.
        """
        if not self.debug_load:
            self.build_stk_contracts()

            tprint('Building option contracts...')
            for stk in self.core.contract_pool['STK']:
                self.core.contract_pool['OPT'].extend(self.build_opt_contracts(stk=stk))

            if self.core.randomize_opts:
                random.shuffle(self.core.contract_pool['OPT'])
                tprint(f'OPT contracts randomized.')

            tprint('Building option contracts ended.')

        current_time = datetime.now().time()
        last_scheduled_update =self.core.exp_update_timer - timedelta(days=1)
        if current_time < self.core.exp_update_timer.time() and self.core.exp_last_update < last_scheduled_update:
            self.get_exp_options()
        else:
            tprint('Generating expired option list skipped because they are up2date.')
        self.option_exp_max_length = len(self.core.contract_pool['EXP'])

        for list_type in ['STK', 'OPT', 'EXP']:
            tprint(f'{list_type} length:{len(self.core.contract_pool[list_type])}')

        self.core.startup = False

    def build_stk_contracts(self):
        """
            Builds stock ContractContainer objects by iterating over the symbols in the underlying list
            and creating a contract container for each symbol.

            Requests conId for each object.
            Conditionally on the conId provided both available option expiries and strikes are saved in the object.

            All stock ContractContainer objects are put into self.core.contract_pool['STK']

            :input: self.core.underlying_list
            :output:  self.core.contract_pool['STK'] :appending

            """
        tprint('Building stock contracts...')
        for symbol in self.core.underlying_list['STK']:

            stk = self.ContractContainer(self.core, symbol=symbol, secType='STK')
            # TODO: Check correct symbol for B shares like BRK.B

            stk.set_reqId_assign(self.core.reqId_1, reqType='ReqConDetails')
            self.tws_con.reqContractDetails(self.core.reqId_1, stk.get_contract())
            self.core.reqId_1 += 1

            while not stk.get_error_flag() and not stk.get_conId():
                pass

            if stk.check_conId():
                stk.set_reqId_assign(self.core.reqId_1, reqType='ReqExpStr')
                self.tws_con.reqSecDefOptParams(self.core.reqId_1, stk.get_symbol(), '', stk.get_secType(), stk.get_conId())
                self.core.reqId_1 += 1

                self.core.contract_pool['STK'].append(stk)
                time_breaker = datetime.now() + timedelta(seconds=5)
                while not stk.get_expiries() and not stk.get_strikes() and datetime.now() < time_breaker:
                    sleep(.1)
                    pass

        self.stk_sorter_pointer = len(self.core.contract_pool['STK'])

        tprint('Building stock contracts ended.')

    def build_opt_contracts(self, stk: 'ContractContainer', expiry: str = None) -> list['ContractContainer']:
        """
        Builds a list of ContractContainer objects representing options contracts from a stock ContractContainer object.
        If provided either uses specific expiry or alternatively loops through all in the stock ContractContainer object archived existing expiries.

        Args:
            stk (ContractContainer): The stock ContractContainer.
            expiry (str, optional): The expiry date. Defaults to None.

        Returns:
            list[ContractContainer]: The list of ContractContainer objects.
        """
        opt_contracts = []

        if expiry:
            expiries = [expiry]
        else:
            expiries = stk.get_expiries()

        for expiry in expiries:
            opt = None
            for strike in stk.get_strikes():
                for right in ['C', 'P']:
                    opt = self.ContractContainer(self.core, symbol=stk.get_symbol(), secType='OPT', strike=strike, right=right, lastTradeDateOrContractMonth=expiry)
                    opt_contracts.append(opt)
                    stk.register_derivative_child(opt)
            if opt:
                self.db.check_table_exists(contract_container=opt)
            else:
                tprint(f'Could not check for table existence for {stk.get_symbol()} OPT on {expiry}.')

        return opt_contracts

    def get_exp_options(self):
        """
        Retrieves expired option contracts from the database.

        This method fetches the table structure from the database, identifies the expired option contracts,
        and updates the contract pool accordingly.

        :input: SQL database and table names
        :output: Appending: self.core.contract_pool['EXP'] :appending | :deleting [optional] (['OPT[)
        """

        tprint('Getting expired option contracts...')
        start = 0 if datetime.now().time() > time(22, 00) else 1
        expiries = [datetime.today().date() - timedelta(days=x) for x in range(start, self.core.expired_opt_days + 1)]
        table_structure = self.db.fetch_all_table_names(return_data=True)
        databases = set(map(lambda x: f'Data_OPT_{x.strftime('%b%y')}', expiries))

        expired_tables = {}
        for database in databases:
            for table in table_structure[database]:
                if datetime.strptime(table.split('_')[2], '%d%b%y').date() in expiries:
                    expired_tables[table] = None

        for table in expired_tables.keys():
            #last_price = self.db.get_last_price(stk_symbol=table.split('_')[0])
            stk_contract = list(filter(lambda x: x.get_symbol() == table.split('_')[0] and x.get_secType() == 'STK', self.core.contract_pool['STK']))
            if stk_contract is not None:
                expiry = datetime.strptime(table.split('_')[2], '%d%b%y').date().strftime('%Y%m%d')
                opt_contract = self.build_opt_contracts(stk=stk_contract[0], expiry=expiry)
                self.core.contract_pool['EXP'].extend(opt_contract)
                if opt_contract in self.core.contract_pool['OPT']:
                    self.core.contract_pool['OPT'].remove(opt_contract)

        tprint('Getting expired option contracts ended.')


    def bit_to_insert(self): # TODO: Implement logical load vs rebuild logic

        print('Loading contract data')
        with open('option_contracts.pkl', 'rb') as file:
            contracts = pickle.load(file)

        print('Contract length:', len(contracts))
        from time import perf_counter
        start_time = perf_counter()

        for i, contract in enumerate(contracts):
            try:
                if i % 100 == 0 and i != 0:
                    perf = perf_counter() - start_time
                    print(f'{perf} Secs. Count: {i}')
            except pyodbc.ProgrammingError:
                continue

    def pipeline_sorter(self):
        """
        Continuously monitors the contract pools and immediate pool,
        ensuring they are populated before proceeding.

        Contract pools hold all newly created contracts. EXP and OPT sub-pools are popped after processing, while STK is in an endless but delayed loop
        STK and OPT contract pools double-check if SQL databases tables exist. Else they are created.
        EXP and OPT contract pools check if all data up to expiry is already archived. Additionally OPT checks for necessity to request data, else it's rescheduled to the end of the pool.

        Very last there is a time-based scheduler to trigger EXP and STK contracts when appropriate. Eg. working days after trading hours

        This function runs indefinitely until the program is stopped.

        :input self.core.contract_pool :popping | Reordering | Index-Loop
        :output elf.core.immediate_pool : appending

        """
        while True:
            while (not self.core.contract_pool['STK'] and not self.core.contract_pool['OPT'] and not self.core.contract_pool['EXP']) or self.core.startup:
                sleep(1)
                pass

            while len(self.core.immediate_pool) < self.core.ip_length:

                if len(self.core.contract_pool['EXP']) > 0:

                    last_update = self.db.get_last_update(contract_container=self.core.contract_pool['EXP'][0], response=True)
                    expiry = self.core.contract_pool['EXP'][0].get_expiry(dt_object=True)

                    if (last_update and last_update < expiry + timedelta(hours=21, minutes=45)) or not last_update: # TODO add timezones for global application
                        #tprint('Adding from EXP.')
                        self.db.check_table_exists(contract_container=self.core.contract_pool['EXP'][0], create_missing=True)
                        self.core.immediate_pool.append(self.core.contract_pool['EXP'].pop(0))
                    else:
                        self.core.contract_pool['EXP'].pop(0)

                    if len(self.core.contract_pool['EXP']) % 1000 == 0:
                        pct_done = ((self.option_exp_max_length - len(self.core.contract_pool['EXP'])) / self.option_exp_max_length) * 100
                        contracts_done = self.option_exp_max_length - len(self.core.contract_pool['EXP'])
                        tprint(f'Expired options progress: {pct_done:.2f}%. Contracts done: {contracts_done}')

                    if not self.core.contract_pool['EXP'] or len(self.core.contract_pool['EXP']) == 0:
                        self.core.exp_last_update = datetime.now().timestamp()
                        set_key(dotenv_path='.env', key_to_set='EXP_LAST_UPDATE', value_to_set=str(self.core.stk_last_update))

                elif len(self.core.contract_pool['STK'][self.stk_sorter_pointer:]) > 0:
                    #tprint('Adding from STK.')
                    self.db.check_table_exists(contract_container=self.core.contract_pool['STK'][self.stk_sorter_pointer], create_missing=True)
                    self.core.immediate_pool.append(self.core.contract_pool['STK'][self.stk_sorter_pointer])
                    #self.core.immediate_pool.append(self.core.contract_pool['STK'].pop(0))
                    self.stk_sorter_pointer += 1

                    if self.stk_sorter_pointer >= len(self.core.contract_pool['STK']):
                        self.core.stk_last_update = datetime.now().timestamp()
                        set_key(dotenv_path='.env', key_to_set='STK_LAST_UPDATE', value_to_set=str(self.core.stk_last_update))

                elif self.core.contract_pool['OPT'] and len(self.core.contract_pool['OPT']) > 0:
                    self.db.check_table_exists(contract_container=self.core.contract_pool['OPT'][0], create_missing=True)
                    #tprint(f'OPT check')
                    last_update = self.db.get_last_update(contract_container=self.core.contract_pool['OPT'][0], response=True)
                    expiry = self.core.contract_pool['OPT'][0].get_expiry(dt_object=True)

                    if expiry > datetime.now():
                        if last_update and not (datetime.now() - last_update) < max(0.5 * (expiry - datetime.now()), timedelta(days=30)):
                            #tprint('Adding from OPT1.')
                            self.core.immediate_pool.append(self.core.contract_pool['OPT'].pop(0))
                        elif last_update and last_update < expiry + timedelta(hours=21, minutes=45):
                            #tprint('Adding from OPT2.')
                            self.core.contract_pool['OPT'].pop(0)
                        elif not last_update:
                            #tprint('Adding from OPT3.')
                            self.core.immediate_pool.append(self.core.contract_pool['OPT'].pop(0))
                        else:
                            #tprint('Adding from OPT4.')
                            self.core.contract_pool['OPT'] = self.core.contract_pool['OPT'][1:].append(self.core.contract_pool['OPT'][0])
                    else:
                        #tprint('Adding from OPT5.')
                        self.core.contract_pool['OPT'] = self.core.contract_pool['OPT'][1:] + [self.core.contract_pool['OPT'][0]]

                sleep(.1)

            if datetime.now().weekday() not in self.core.timer_exclude_days:
                if datetime.now() >= self.core.stk_update_timer:
                    tprint('Stk update timer triggered.')
                    self.core.stk_update_timer += timedelta(days=1)
                    self.stk_sorter_pointer = 0
                elif datetime.now() >= self.core.exp_update_timer:
                    tprint('Exp update timer triggered.')
                    self.get_exp_options()
                    self.option_exp_max_length = len(self.core.contract_pool['EXP'])

                    self.core.exp_update_timer += timedelta(days=1)
