from datetime import datetime
from ibapi.contract import Contract
from database_broker import DatabaseBroker
from typing import NoReturn

from core import tprint

class ContractContainer:
    def __init__(self, core, **kwargs):
        self.core = core

        stk_cond: bool = all([x in kwargs.keys() for x in ['symbol', 'secType']])
        opt_cond: bool = all([x in kwargs.keys() for x in ['symbol', 'secType', 'strike', 'right', 'lastTradeDateOrContractMonth']])

        if not stk_cond and not opt_cond: # TODO: Change condition
            raise Exception('DataContainer: Invalid input to create security contract.')

        self.price_data = {}
        self.conId = None

        self.child_container: list['ContractContainer'] = []

        self.build_contract(**kwargs)

        self.strikes, self.expiries = [], []

        self.last_update = None

        self.error_flag = False
        self.historical_data_end = False

        self.db = DatabaseBroker(self.core, self)

    def __str__(self):
        match self.contract.secType:
            case 'STK':
                return f'<Data Container Instance> {self.contract.symbol} STK.'
            case 'OPT':
                dt_s: str = datetime.strptime(self.contract.lastTradeDateOrContractMonth, "%Y%m%d").strftime("%d%b%y")
                return f'<Data Container Instance> {self.contract.symbol} {self.contract.strike}{self.contract.right} {dt_s} OPT.'

    def build_contract(self, **kwargs):
        self.contract: Contract = Contract()
        self.contract.symbol = kwargs['symbol']
        self.contract.secType = kwargs['secType']
        self.contract.exchange = 'SMART'
        self.contract.currency = 'USD'

        if kwargs['secType'] == 'OPT':
            self.contract.strike = kwargs['strike'] if 'strike' in kwargs.keys() else None
            self.contract.right = kwargs['right'] if 'right' in kwargs.keys() else None
            self.contract.lastTradeDateOrContractMonth = kwargs['lastTradeDateOrContractMonth'] if 'lastTradeDateOrContractMonth' in kwargs.keys() else None

    def get_last_price(self) -> float:
        return self.price_data[list(self.price_data.keys())[-1]][0]

    def get_price_data(self) -> dict[datetime, list]:
        return self.price_data

    def set_price_data(self, prices: dict[datetime, list]):
        self.price_data[list(prices.keys())[0]] = list(prices.values())[0]
        #print('Received: ', self.price_data)

    def check_conId(self) -> bool:
        if self.contract.secType == 'STK' and self.conId is None:
            return False
        return True

    def get_conId(self) -> int:
        return self.conId

    def set_conId(self, conId: int):
        self.conId = conId

    def get_contract(self) -> Contract:
        return self.contract

    def get_secType(self) -> str:
        return self.contract.secType

    def get_right(self) -> str:
        return self.contract.right

    def get_strike(self) -> int:
        if self.contract.secType == 'STK':
            raise Exception(f'Strike only available for contract instances of secType OPT. Requested {self.contract.symbol} of type {self.contract.secType}.')
        return self.contract.strike

    def get_symbol(self) -> str:
        return self.contract.symbol

    def get_expiries(self) -> list[int]:
        if self.contract.secType != 'STK':
            raise Exception(f'Expiry lists only available for contract instances of secType STK. Requested {self.contract.symbol} of type {self.contract.secType}.')
        elif not self.expiries:
            #tprint(f'No expiry data available for {self.contract.symbol}.')
            return []
        else:
            return self.expiries

    def get_expiry(self, dt_object: bool = False, output_str_format: str = '%Y%m%d') -> datetime | str | None:
        if self.contract.secType != 'OPT':
            raise Exception(f'Expiry date only available for contract instances of secType OPT. Requested {self.contract.symbol} of type {self.contract.secType}.')

        dt = datetime.strptime(self.contract.lastTradeDateOrContractMonth, '%Y%m%d')
        if dt_object:
            return dt
        else:
            return dt.strftime(output_str_format)

    def get_strikes(self) -> list:
        if self.contract.secType != 'STK':
            raise Exception(f'Expiry dates only available for contract instances of secType STK. Requested {self.contract.symbol} of type {self.contract.secType}.')

        if not self.strikes:
            return []
        else:
            return self.strikes

    def set_strexp(self,  expiries: list[str], strikes: list[int]):
        for x in expiries:
            if x not in self.expiries: self.expiries.append(x)
        for x in strikes:
            if x not in self.strikes: self.strikes.append(x)

    def set_reqId_assign(self, reqId: int, reqType: str):
        """
        Assigns a request ID to a specific request type.

        Args:
            reqId (int): The request ID to be assigned.
            reqType (str): The method to be executed once data is received
                Supported methods:  ReqHistData -> self.set_price_data
                                    ReqConDetails -> self.set_conId
                                    ReqExpStr -> self.set_strexp
        Raises:
            AttributeError: If the reqType is not one of the valid options.

        Returns:
            None
        """
        match reqType:
            case 'ReqHistData':
                self.core.reqId_hashmap[reqId] = self.set_price_data
            case 'ReqConDetails':
                self.core.reqId_hashmap[reqId] = self.set_conId
            case 'ReqExpStr':
                self.core.reqId_hashmap[reqId] = self.set_strexp
            case _:
                raise AttributeError('Invalid reqType. Valid options: ReqHistData, ReqConDetails, ReqExpStr')

    def register_derivative_child(self, child: 'ContractContainer'):
        self.child_container.append(child)

    def get_last_update(self, response = True) -> datetime | NoReturn:
        if not self.last_update:
            self.last_update = self.db.get_last_update(contract_container=self)
        if response:
            return self.last_update

    def get_database(self, ** kwargs) -> str:
        match self.contract.secType:
            case 'STK':
                return f'Data_STK'
            case 'OPT':
                return f'Data_OPT_{datetime.strptime(self.contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%b%y')}'
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

    def get_table(self, **kwargs) -> str:
        match self.contract.secType:
            case 'STK':
                return f'{self.contract.symbol.replace(".", "")}_STK'
            case 'OPT':
                return f'{self.contract.symbol}_OPT_{datetime.strptime(self.contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime("%d%b%y")}'
            case _:
                raise KeyError('Security type not supported. Valid secTypes: STK, OPT')

    def set_error_flag(self, flag=False,**kwargs):
        #print(f'Error flag set for {self.contract.symbol}.')
        self.error_flag = flag

    def get_error_flag(self, **kwargs) -> bool:
        return self.error_flag

    def set_historical_data_end(self, flag=False, **kwargs):
        self.historical_data_end = flag

    def get_historical_data_end(self, **kwargs) -> bool:
        return self.historical_data_end



