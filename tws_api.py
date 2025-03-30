from datetime import datetime
import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from threading import Thread

from core import tprint


class TWSCon(EWrapper, EClient):

    def __init__(self, core):
        super().__init__()
        EClient.__init__(self, wrapper=self)

        self.core = core

        self.t: Thread | None = None
        core.write_tws_connection(self)

        self.build_connection()

    def connectAck(self):
        tprint(f'Connected to TWS API.')

    def connectionClosed(self):
        tprint('Disconnected from TWS API.')

    def error(self, reqId, errorCode, errorString):
        tprint(f'Error: {errorCode} --> {errorString}', debug=True)
        if errorCode in [162, 200]:
            self.core.reqId_hashmap[reqId].__self__.set_error_flag(flag=True)

        # if errorCode == 504:
        #     tprint(f'Error thread: {self.get_instance_info(self.t)}, {self.isConnected()}', debug=True)

    def build_connection(self):
        while True:
            try:
                self.connect(self.core.host_ip, self.core.api_port, self.core.client_id)
                time.sleep(2)
                self.t: Thread = Thread(target=self.run)
                self.t.start()
                time.sleep(10)

                if self.isConnected():
                    #self.core.connection_status = ConnectionStatus.CONNECTED
                    break

            except AttributeError as e:
                tprint(f'Attribute error: {e}', debug=True)

    def historicalData(self, reqId, bar):
        self.core.last_receive = datetime.now()
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId]({bar.date: {'Open': bar.open, 'High': bar.high, 'Low': bar.low, 'Close': bar.close}})

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        self.core.reqId_hashmap[reqId].__self__.set_historical_data_end(flag=True)

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes):
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](expiries=list(expirations) or [], strikes=list(strikes) or [])

    def contractDetails(self, reqId: int, contractDetails):
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](contractDetails.contract.conId)

