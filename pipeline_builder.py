from collections import defaultdict
from datetime import datetime, time, timedelta
from itertools import batched
import os
import pickle
from random import randint, shuffle
from threading import Thread
from time import sleep

from constituents_handler import constituents_list_updater, load_constituents
from contract_container import ContractContainer
from core import Core, EnvDistributor, tprint, write_data_json
from database_broker import DatabaseBroker
from tws_api import TWSCon


class PipelineBuilder:
    def __init__(self):
        self.core: Core = EnvDistributor.get_core()

        self.ContractContainer: type[ContractContainer] = ContractContainer

        self.db: DatabaseBroker = DatabaseBroker()

        self.tws_con: TWSCon = self.core.tws_con

        self.t1: Thread = Thread(target=self.pipeline_sorter)
        self.t1.start()

        self.option_exp_max_length: int = 0  # max length of current expired options batch
        self.stk_sorter_pointer: int = 0  # queue pointer to the current index of self.core.contract_pool['STK']

        constituents_list_updater(self.core)
        load_constituents(self.core)

    def startup_build_sequence(self):
        """
        Orchestrates startup build of all self.core.contract_pool['STK' | 'EXP' | 'OPT'] security instance types
            1. Builds stock contracts
            2. Builds option contracts
            2.1 Conditionally (self.core.RANDOMIZE_OPTS) randomizes option contracts
            3. Conditionally  (self.core.exp_update_timer & self.core.exp_last_update) builds expired option contracts

        If finished sets flag (self.core.startup) for main purpose threads to start.

        :input:     core variable space and instance variables
        :output:    :boolswitch self.core.startup flag
        """

        self.build_stk_contracts()
        opt_save_file_exists = os.path.exists(os.path.join(os.path.dirname(__file__), self.core.MAIN_OPT_FILE_NAME))
        if self.core.last_opt_build > datetime.now() - timedelta(days=self.core.OPT_LIST_CURRENT) and opt_save_file_exists:
            tprint('Loading prior option contracts.')
            self.load_options_from_file(main_list=True)
        else:
            tprint('Building option contracts...')
            for stk in self.core.contract_pool['STK']:
                sorted_options = self.build_opt_contracts(stk=stk)
                for k, v in sorted_options.items():
                    self.core.contract_pool['OPT'][k].extend(v)

            self.core.last_opt_build = datetime.now()

            if self.core.RANDOMIZE_OPTS:
                for k in self.core.contract_pool['OPT'].keys():
                    shuffle(self.core.contract_pool['OPT'][k])
                tprint(f'Option contracts randomized.')

            tprint('Building option contracts ended.')
            write_data_json(self.core, data={'LAST_OPT_BUILD': self.core.last_opt_build})
            self.dump_options_to_file(main_list=True)

        current_time = datetime.now().time()
        last_scheduled_update = self.core.exp_update_timer - timedelta(days=1)
        if (current_time < self.core.exp_update_timer.time() and self.core.exp_last_update < last_scheduled_update) or self.core.FORCE_EXP_UPDATE:
            self.get_exp_options()

        # elif datetime.today().weekday() in [5, 6]:
        #     'expired_option_contracts.pkl'
        else:
            tprint('Generating expired option list skipped because it is up to date.')

        self.option_exp_max_length = len(self.core.contract_pool['EXP'])

        tprint(f'Start Stock queue length: {len(self.core.contract_pool['STK']):,}')
        tprint(f'Start Option queue length: {sum(len(x) for x in self.core.contract_pool['OPT'].values()):,}')
        tprint(f'Start Expiry queue length: {len(self.core.contract_pool['EXP']):,}')

        self.core.startup = False

    def build_stk_contracts(self):
        """
        Builds stock ContractContainer objects by iterating over the symbols list.

        Prepares stock contract instances for building of derivative ContractContainer objects.
            Requests conId for each object.
            Conditionally on the conId provided both available option expiries and strikes are archived in the object.

        :input:     core variable space and instance variables
        :output:    :appending to self.core.contract_pool['STK']
        """

        tprint('Building stock contracts...', )
        for symbol in self.core.underlying_list['STK']:
            stk = self.ContractContainer(symbol=symbol, secType='STK')

            stk.set_reqId_assign(self.core.reqId_1, reqType='reqConDetails')
            self.tws_con.reqContractDetails(self.core.reqId_1, stk.get_contract())
            self.core.reqId_1 += 1

            while not stk.get_error_flag() and not stk.get_conId():
                pass

            if stk.check_conId():
                stk.set_reqId_assign(self.core.reqId_1, reqType='reqExpStr')
                self.tws_con.reqSecDefOptParams(self.core.reqId_1, stk.get_symbol(), '', stk.get_secType(), stk.get_conId())
                self.core.reqId_1 += 1
                self.core.contract_pool['STK'].append(stk)
                time_breaker = datetime.now() + timedelta(seconds=5)
                while not stk.get_expiries() and not stk.get_strikes() and datetime.now() < time_breaker:
                    sleep(.1)
                    pass

        self.stk_sorter_pointer = len(self.core.contract_pool['STK'])

        tprint('Building stock contracts ended.')

    def build_opt_contracts(self, stk: ContractContainer, expiry: str = None) -> list[ContractContainer] | dict[int, ContractContainer]:
        """
        Builds a list of ContractContainer objects representing options contracts from a stock ContractContainer object.
        If provided either uses specific expiry or alternatively loops through all in the stock ContractContainer object archived existing expiries.

        :input:     core variable space and instance variables
                    stk (ContractContainer): The security (stock) ContractContainer.
                    expiry (str, optional): The expiry date. Defaults to None.

        :output:    :return opt_contracts (list[ContractContainer]): The list of ContractContainer instances.

        """
        opt_contracts = []

        if expiry:
            expiries = [expiry]
        else:
            expiries = stk.get_expiries()

        for expiration in expiries:
            opt = None
            for strike in stk.get_strikes():
                for right in ['C', 'P']:
                    opt = self.ContractContainer(symbol=stk.get_symbol(), secType='OPT', strike=strike, right=right, lastTradeDateOrContractMonth=expiration)
                    opt_contracts.append(opt)
                    stk.register_derivative_child(opt)

            if opt:
                self.db.check_table_exists(contract_container=opt)
            else:
                tprint(f'Could not check if table exists for {stk.get_symbol()} OPT on {expiry}.')

        if expiry:
            return opt_contracts

        sorted_contracts = {}
        current_stk_price = stk.get_last_price()
        for opt_contract in opt_contracts:
            date_dif = opt_contract.get_expiry(dt_object=True) - datetime.today()
            date_dif = date_dif.days if date_dif.days > 0 else 0
            target_price = opt_contract.get_strike()

            # linear priority ranges
            for i in range(1, 10):
                if i not in sorted_contracts.keys():
                    sorted_contracts[i] = []

                higher_start = current_stk_price * (1 + i / 50)
                higher_end = current_stk_price * (1 + i / 10)
                higher_m = (higher_end - higher_start) / 360

                lower_start = current_stk_price * (1 - i / 50)
                lower_end = lower_start - current_stk_price * (1 - i / 10)
                lower_m = (lower_end - lower_start) / 360

                under_upper = target_price < (higher_start + higher_m * date_dif)
                over_lower = target_price > (lower_start + lower_m * date_dif)
                if under_upper and over_lower:
                    sorted_contracts[i].append(opt_contract)
                    break

        return sorted_contracts

    def get_exp_options(self, force_update: bool = False):
        """
        Retrieves expired option contracts from the database and updates the contract pool.

        This method checks if expired option contracts can be loaded from a file. If not, it fetches the table structure from the database,
        identifies the expired option contracts, and updates the contract pool accordingly.

        :input:     core variable space and instance variables
        :output:    :appending to self.core.contract_pool['EXP']
                    :removing [optional] from self.core.contract_pool['OPT']
        """

        exp_options_loaded = False
        try:
            m_time = os.path.getmtime(self.core.EXP_OPT_FILE_NAME)
            weekend_cond = (datetime.today() - datetime.fromtimestamp(m_time)) <= timedelta(hours=60) and datetime.fromtimestamp(m_time).weekday() in [4, 5, 6]
            workday_cond = (datetime.today() - datetime.fromtimestamp(m_time)) <= timedelta(hours=18) and datetime.fromtimestamp(m_time).weekday() not in [4, 5, 6]
            if weekend_cond or workday_cond:
                self.load_options_from_file(expired_list=True)
                exp_options_loaded = True

        except (FileNotFoundError, EOFError):
            pass

        if not exp_options_loaded or force_update:
            tprint('Getting expired option contracts...')
            start = 0 if datetime.now().time() > time(22, 00) else 1
            expiries = [datetime.today().date() - timedelta(days=x) for x in range(start, self.core.EXPIRED_OPT_DAYS + 1)]
            table_structure = self.db.fetch_all_table_names(return_data=True)
            databases = set(map(lambda x: f'Data_OPT_{x.strftime('%b%y')}', expiries))

            expired_tables = {}
            for database in databases:
                for table in table_structure[database]:
                    if datetime.strptime(table.split('_')[2], '%d%b%y').date() in expiries:
                        expired_tables[table] = None

            exp_order = defaultdict(list)
            for table in expired_tables.keys():
                #last_price = self.db.get_last_price(stk_symbol=table.split('_')[0])
                try:
                    stk_contract = list(filter(lambda x: x.get_symbol() == table.split('_')[0] and x.get_secType() == 'STK', self.core.contract_pool['STK']))[0]
                    if stk_contract is not None:
                        expiry = datetime.strptime(table.split('_')[2], '%d%b%y').date().strftime('%Y%m%d')
                        opt_contracts = self.build_opt_contracts(stk=stk_contract, expiry=expiry)

                        underlying_last_price = stk_contract.get_last_price()
                        try:
                            opt_contracts = sorted(opt_contracts, key=lambda x: abs(underlying_last_price - x.get_strike()))
                        except TypeError:
                            tprint(f'No stock pricing data available for {stk_contract}.')

                        for i, contract_batch in enumerate(batched(opt_contracts, n=2)):
                            exp_order[i].extend(contract_batch)
                            for contract in contract_batch:
                                for k in self.core.contract_pool['OPT'].keys():
                                    if contract in self.core.contract_pool['OPT'][k]:
                                        self.core.contract_pool['OPT'][k].remove(contract)
                except IndexError:
                    tprint(f'Index error for {table.split('_')[0]}.')
                    continue

            for key in exp_order.keys():
                for contract in exp_order[key]:
                    if contract not in self.core.contract_pool['EXP']:
                        self.core.contract_pool['EXP'].append(contract)

            tprint('Getting expired option contracts ended.')

            self.dump_options_to_file(expired_list=True)

    def pipeline_sorter(self):
        """
        Continuously sorts and updates the contract pools to ensure they are populated and ready for processing.

        This method runs indefinitely, checking the status of the contract pools and performing the following actions:

        1. Waits until the startup process is complete and the contract pools are populated.
        2. Continuously checks the length of the immediate pool and populates it with contracts from the EXP pool if necessary.
        3. Updates the contract pools by removing expired contracts and adding new ones.

        Checks and triggers time-controlled actions:
            1. Stock updater
            2. Expired options updater
            3. Post weekend / Monday roll

        :input:     core variable space and instance variables
        :output:    :appending to self.core.immediate pool
                    :removing from self.core.contract_pool['STK' | 'EXP' | 'OPT']
        """

        while (not self.core.contract_pool['STK'] and not self.core.contract_pool['OPT'] and not self.core.contract_pool['EXP']) or self.core.startup:
            sleep(1)

        while True:
            while len(self.core.immediate_pool) == self.core.IP_LENGTH:
                sleep(0.1)

            while len(self.core.immediate_pool) < self.core.IP_LENGTH:
                tprint(f'Primal pool lengths: {len(self.core.contract_pool["STK"]), self.stk_sorter_pointer, sum(len(x) for x in self.core.contract_pool['OPT'].values()), len(self.core.contract_pool['EXP']), len(self.core.immediate_pool)}', debug=True)

                if len(self.core.contract_pool['EXP']) > 0:
                    tprint(f'EXP Option condition met.', debug=True)
                    last_update = self.db.get_last_update(contract_container=self.core.contract_pool['EXP'][0], response=True)
                    expiry = self.core.contract_pool['EXP'][0].get_expiry(dt_object=True)

                    if (last_update and last_update < expiry + timedelta(hours=21, minutes=45)) or not last_update:
                        self.db.check_table_exists(contract_container=self.core.contract_pool['EXP'][0], create_missing=True)
                        self.core.immediate_pool.append(self.core.contract_pool['EXP'].pop(0))
                        tprint(f'EXP Option added to immediate pool.', debug=True)

                        if len(self.core.contract_pool['EXP']) > 0 and len(self.core.contract_pool['EXP']) % 1000 == 0:
                            pct_done = ((self.option_exp_max_length - len(self.core.contract_pool['EXP'])) / self.option_exp_max_length) * 100
                            contracts_done = self.option_exp_max_length - len(self.core.contract_pool['EXP'])
                            tprint(f'Expired options progress: {pct_done:.2f}%. Contracts done: {contracts_done:,}')

                            if datetime.today().weekday() in [5, 6]:
                                self.dump_options_to_file(expired_list=True)

                        if not self.core.contract_pool['EXP'] or len(self.core.contract_pool['EXP']) == 0:
                            self.core.exp_last_update = datetime.now()
                            write_data_json(self.core, data={'EXP_LAST_UPDATE': self.core.exp_last_update})

                    else:
                        self.core.contract_pool['EXP'].pop(0)
                        continue

                elif len(self.core.contract_pool['STK'][self.stk_sorter_pointer:]) > 0:
                    tprint(f'STK condition met.', debug=True)

                    self.db.check_table_exists(contract_container=self.core.contract_pool['STK'][self.stk_sorter_pointer], create_missing=True)
                    self.core.immediate_pool.append(self.core.contract_pool['STK'][self.stk_sorter_pointer])
                    #self.core.immediate_pool.append(self.core.contract_pool['STK'].pop(0))
                    self.stk_sorter_pointer += 1

                    if self.stk_sorter_pointer >= len(self.core.contract_pool['STK']):
                        self.core.stk_last_update = datetime.now()

                        write_data_json(self.core, data={'STK_LAST_UPDATE': self.core.stk_last_update})

                elif self.core.contract_pool['OPT'] and sum(len(x) for x in self.core.contract_pool['OPT'].values()) > 0:
                    tprint(f'OPT option condition 1 met.', debug=True)
                    rand_choice = randint(0, 100) / 100
                    for k, v in self.core.probability_table.items():
                        if rand_choice <= v:
                            rand_key = k
                            break

                    self.db.check_table_exists(contract_container=self.core.contract_pool['OPT'][rand_key][0], create_missing=True)
                    last_update = self.db.get_last_update(contract_container=self.core.contract_pool['OPT'][rand_key][0], response=True)
                    expiry = self.core.contract_pool['OPT'][rand_key][0].get_expiry(dt_object=True)

                    if expiry > datetime.now():
                        tprint(f'OPT option condition 2 met.', debug=True)
                        if last_update and not (datetime.now() - last_update) < max(0.5 * (expiry - datetime.now()), timedelta(days=30)):
                            self.core.immediate_pool.append(self.core.contract_pool['OPT'][rand_key].pop(0))
                            tprint(f'OPT Option added to immediate pool 1.', debug=True)
                        elif last_update and last_update < expiry + timedelta(hours=21, minutes=45):
                            self.core.contract_pool['OPT'][rand_key].pop(0)
                            tprint(f'EXP Option withdrawn.', debug=True)
                        elif not last_update:
                            self.core.immediate_pool.append(self.core.contract_pool['OPT'][rand_key].pop(0))
                            tprint(f'OPT Option added to immediate pool 2.', debug=True)
                        else:
                            print('Ever triggered?')
                            #self.core.contract_pool['OPT'][rand_key].pop(0) = self.core.contract_pool['OPT'][1:].append(self.core.contract_pool['OPT'][0])
                    else:
                            tprint(f'Expired option {expiry}', debug=True)
                            self.core.contract_pool['OPT'][rand_key].pop(0)
                            #self.core.contract_pool['EXP'].append(self.core.contract_pool['OPT'][rand_key].pop(0))

                    remaining_length = sum(len(x) for x in self.core.contract_pool['OPT'].values())
                    if remaining_length % 5000 == 0:
                        tprint(f'Option pool remaining length: {remaining_length:,}')
                        self.dump_options_to_file(main_list=True)
                    elif remaining_length % 1000 == 0:
                        tprint(f'Option pool remaining length: {remaining_length:,}')

                sleep(.1)

            if datetime.now().weekday() not in self.core.TIMER_EXCLUDE_DAYS:
                if datetime.now() >= self.core.stk_update_timer > self.core.stk_last_update:
                    tprint('STK update timer triggered.')
                    self.core.stk_update_timer += timedelta(days=1)
                    self.stk_sorter_pointer = 0
                elif datetime.now() >= self.core.exp_update_timer:
                    tprint('EXP update timer triggered.')
                    self.get_exp_options(force_update=True)
                    self.option_exp_max_length = len(self.core.contract_pool['EXP'])

                    self.core.exp_update_timer += timedelta(days=1)
                elif datetime.now() >= self.core.monday_roll_timer:
                    tprint('Monday roll timer triggered.')
                    self.core.contract_pool['EXP'] = []
                    self.dump_options_to_file(expired_list=True)
                    self.core.monday_roll_timer += timedelta(days=7)

    def dump_options_to_file(self, main_list: bool = False, expired_list: bool = False):
        """
        Dumps option files into a Pickle file.
        File save process is wrapped into disconnecting and reconnecting of core space for each contract.
        Keeps looping until successfully finished.

        :input:     core variable space and instance variables
                    main_list (optional): boolswitch to indicate if main option list to be dumped
                    expired_list (optional): boolswitch to indicate if expired option list to be dumped
        :output:    :creating expired options Pickle dump file
        """

        selection_list = []
        if main_list:
            selection_list.append(('main', self.core.MAIN_OPT_FILE_NAME, self.core.contract_pool['OPT']))
        if expired_list:
            selection_list.append(('expired', self.core.EXP_OPT_FILE_NAME, self.core.contract_pool['EXP']))

        for type_name, file_name, contract_list in selection_list:
            with open(file_name, 'wb') as file:
                while True:
                    try:
                        match type_name:
                            case 'expired':
                                for contract in contract_list:
                                    contract.disconnect_core_space()

                                pickle.dump(contract_list, file)
                                tprint(f'{len(contract_list):,} {type_name} options saved to {file_name}.')

                                for contract in contract_list:
                                    contract.connect_core_space(self.core)

                                break
                            case 'main':
                                for k, sublist in contract_list.items():
                                    for contract in sublist:
                                        contract.disconnect_core_space()

                                pickle.dump(contract_list, file)

                                tprint(f'{len(contract_list):,} {type_name} options saved to {file_name}.')

                                for k, sublist in contract_list.items():
                                    for contract in sublist:
                                        contract.connect_core_space(self.core)

                                break

                    except RuntimeError:
                        tprint(f'Failed to save {type_name} options. Trying again in 10 seconds...')
                        sleep(10)

    def load_options_from_file(self, main_list: bool = False, expired_list: bool = False):
        """
         Loads option files from a Pickle file.
         After loading securities are reconnected to core space by each contract.

         :input:     core variable space and instance variables
                     main_list (optional): boolswitch to indicate if main option list to be dumped
                     expired_list (optional): boolswitch to indicate if expired option list to be dumped
         :output:    :fills selected option queue lists with previously created and saved security instances
        """

        selection_list = []
        if main_list:
            selection_list.append(('main', self.core.MAIN_OPT_FILE_NAME, 'OPT'))
        if expired_list:
            selection_list.append(('expired', self.core.EXP_OPT_FILE_NAME, 'EXP'))

        for type_name, file_name, contract_list in selection_list:
            with open(file_name, 'rb') as f:
                contract_data = pickle.load(f)

            match type_name:
                case 'expired':
                    tprint(f'{len(contract_data):,} {type_name} options loaded from {file_name}.')

                    for contract in contract_data:
                        contract.connect_core_space(self.core)

                    self.core.contract_pool[contract_list] = contract_data

                    tprint(f'{type_name.capitalize()} contracts loaded and reconnected to Core space.')

                case 'main':
                    tprint(f'{sum(len(x) for x in contract_data.values()):,} {type_name} options loaded from {file_name}.')

                    for k, sublist in contract_data.items():
                        for contract in sublist:
                            contract.connect_core_space(self.core)

                    for k in contract_data.keys():
                        self.core.contract_pool[contract_list][k] = contract_data[k]

                    tprint(f'{type_name.capitalize()} contracts loaded and reconnected to Core space.')
