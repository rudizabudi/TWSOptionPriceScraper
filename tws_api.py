from datetime import datetime, timedelta
from enum import Enum, auto
from threading import Thread
import time
from typing import NoReturn

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from core import tprint, start_ibgateway


class TWSCon(EWrapper, EClient):

    def __init__(self, core):
        super().__init__()
        EClient.__init__(self, wrapper=self)

        self.core = core

        self.t: Thread | None = None
        core.tws_con = self

        self.connection_status: Enum = ConnectionStatus.DISCONNECTED

        self.build_connection()

    def connectAck(self):
        if self.connection_status.name == 'DISCONNECTED':
            self.connection_status = ConnectionStatus.CONNECTED
            self.core.last_connection = datetime.now()
            tprint(f'Connected to TWS API.')

    def connectionClosed(self):
        if self.connection_status.name == 'CONNECTED' or not self.core.time_disconnect:
            self.core.time_disconnect = datetime.now()
            self.connection_status = ConnectionStatus.DISCONNECTED
            tprint(f'Disconnected from TWS API.')

        if self.connection_status.name == 'DISCONNECTED':
            if self.core.time_disconnect + timedelta(seconds=self.core.RESTART_THRESHOLD) <= datetime.now():
                start_ibgateway(self.core)
                time.sleep(30)

    def error(self, reqId, errorCode, errorString) -> NoReturn:
        tprint(f'Error: {errorCode} --> {errorString}', debug=True)
        if errorCode in [162, 200]:
            self.core.reqId_hashmap[reqId].__self__.set_error_flag(flag=True)

        # if errorCode == 504:
        #     tprint(f'Error thread: {self.get_instance_info(self.t)}, {self.isConnected()}', debug=True)

    def build_connection(self) -> NoReturn:
        while True:
            try:
                self.connect(self.core.HOST_IP, self.core.API_PORT, self.core.CLIENT_ID)
                time.sleep(2)
                self.t: Thread = Thread(target=self.run)
                self.t.start()
                time.sleep(10)

                if self.isConnected():
                    #self.core.connection_status = ConnectionStatus.CONNECTED
                    break

            except AttributeError as e:
                tprint(f'Attribute error: {e}', debug=True)

    def historicalData(self, reqId, bar) -> NoReturn:
        self.core.last_receive = datetime.now()
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security instance.')

        self.core.reqId_hashmap[reqId]({bar.date: {'Open': bar.open, 'High': bar.high, 'Low': bar.low, 'Close': bar.close}})

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> NoReturn:
        super().historicalDataEnd(reqId, start, end)
        self.core.reqId_hashmap[reqId].__self__.set_historical_data_end(flag=True)

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes) -> NoReturn:
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](expiries=list(expirations) or [], strikes=list(strikes) or [])

    def contractDetails(self, reqId: int, contractDetails) -> NoReturn:
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](contractDetails.contract.conId)


class ConnectionStatus(Enum):
    DISCONNECTED = auto()
    CONNECTED = auto()

