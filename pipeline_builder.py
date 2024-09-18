from datetime import datetime, timedelta
import pickle
from time import sleep
from threading import Thread

import pyodbc

class PipelineBuilder:
    def __init__(self, core =None, tws_con=None, CC=None, DB=None):
        if None in (core, tws_con, CC, DB):
            raise Exception('<PipelineBuilder INIT> All parameters must be specified.')

        self.core = core
        self.tws_con = tws_con
        self.ContractContainer = CC

        self.db = DB(core=self.core, CC=self.ContractContainer)

    def build_all_contracts(self):
        # Stock contracts plus conIds and Strikes/Expiries

        for symbol in self.core.underlying_list['STK']:

            stk = self.ContractContainer(self.core, symbol=symbol, secType='STK')

            self.core.response_state = 0

            stk.set_reqId_assign(self.core.reqId_1, reqType='ReqConDetails')
            self.tws_con.reqContractDetails(self.core.reqId_1, stk.get_contract())
            self.core.reqId_1 += 1

            while self.core.response_state == 0:
                pass

            if stk.check_conId():
                stk.set_reqId_assign(self.core.reqId_1, reqType='ReqExpStr')
                self.tws_con.reqSecDefOptParams(self.core.reqId_1, stk.get_symbol(), '', stk.get_secType(), stk.get_conId())
                self.core.reqId_1 += 1

            self.core.contract_pool['STK'].append(stk)

            time_breaker = datetime.now() + timedelta(seconds=5)
            while not stk.get_expiry_list() and not stk.get_strikes() and datetime.now() < time_breaker:
                sleep(.1)
                pass

        #Move db check to here?
        for stk in self.core.contract_pool['STK']:
            for expiry in stk.get_expiry_list():
                for strike in stk.get_strikes():
                    for right in ['C', 'P']:
                        opt = self.ContractContainer(self.core, symbol=stk.get_symbol(), secType='OPT', strike=strike, right=right, lastTradeDateOrContractMonth=expiry)
                        self.core.contract_pool['OPT'].append(opt)
                        stk.register_derivate_child(opt)
            self.db.check_table_exists(contract_container=opt)

        print(f'{datetime.now().strftime('%H:%M:%S')} : Fertig.')
        print(len(self.core.contract_pool['STK']), len(self.core.contract_pool['OPT']))

        with open('stock_contracts.pkl', 'wb') as file:
            pickle.dump(self.core.contract_pool['STK'], file)
            print(datetime.now().strftime('%H:%M:%S') + ' :  Stock contracts saved.')

        with open('option_contracts.pkl', 'wb') as file:
            pickle.dump(self.core.contract_pool['OPT'], file)
            print(datetime.now().strftime('%H:%M:%S') + ' :  Option contracts saved.')


    def tester(self):

        print('Loading contract data')
        with open('option_contracts.pkl', 'rb') as file:
            contracts = pickle.load(file)

        print('Contract length:', len(contracts))
        from time import perf_counter, sleep
        start_time = perf_counter()

        expired_options = []
        update_threshold = timedelta(days=3)
        for i, contract in enumerate(contracts):
            expiry = contract.get_expiry(dt_object=True)
            expired = datetime.today() - update_threshold <= expiry <= datetime.today()
            if expired:
                last_update = contract.get_last_update(response=True) or datetime(2020, 1, 1)
                update_needed = (last_update < expiry + timedelta(hours=21, minutes=45)) or False
                if update_needed:
                    self.core.contract_pool['EXP'].append(contract)




        print('Fertig')

        sleep(9999999)

        for i, contract in enumerate(contracts):
            try:
                if i % 100 == 0 and i != 0:
                    perf = perf_counter() - start_time
                    print(f'{perf} Secs. Count: {i}')
                last_update = contract.get_last_update(response=True)
                # print('Last update:')
                # print(i, last_update)
                # print(' - - - ')
            except pyodbc.ProgrammingError:
                continue

    def pipeline_handler(self):
        # Trigger from INIT in thread
        while True:
            while not self.core.contract_pool['STK'] and not self.core.contract_pool['OPT'] and not self.core.contract_pool['EXP']:
                sleep(1)
                pass

            while len(self.core.immediate_pool) < self.core.ip_length:
                if len(self.core.contract_pool['EXP']) > 0:
                    self.core.immediate_pool.append(self.core.contract_pool['EXP'].pop(0))
                elif len(self.core.contract_pool['STK']) > 0:
                    self.core.immediate_pool.append(self.core.contract_pool['STK'].pop(0))
                elif len(self.core.contract_pool['OPT']) > 0:
                    self.core.immediate_pool.append(self.core.contract_pool['OPT'].pop(0))

                if len(self.core.immediate_pool) >= self.core.ip_length:
                    break
