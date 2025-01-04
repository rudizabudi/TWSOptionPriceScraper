from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from threading import Thread

import time

from core import tprint, ConnectionStatus

class TWSCon(EWrapper, EClient):

    def __init__(self, core):
        super().__init__()
        EClient.__init__(self, wrapper=self)

        self.core = core
        self.core.no_contract = False

        tprint('Prebuild')
        self.build_connection()
        tprint('Postbuild')
        self.t: Thread = Thread(target=self.run)
        self.t.start()

    def connectAck(self):
        tprint('Connected TWS API.')
        #tprint(self.core.__dir__())
        self.core.connection_status = ConnectionStatus.CONNECTED

    def connectionClosed(self):
        tprint('Disconnected TWS API.')
        self.core.connection_status = ConnectionStatus.DISCONNECTED
        self.disconnect()
        time.sleep(2)
        self.build_connection()

    def error(self, reqId, errorCode, errorString):
        #print(errorCode, errorString)
        if errorCode in [162, 200]:
            self.core.reqId_hashmap[reqId].__self__.set_error_flag(flag=True)

            #tprint(f'Error {reqId} - {errorCode}: {errorString}')
            #tprint(f'Error keys: {self.core.reqId_hashmap.keys()}')
            #try:
                #tprint(f'Error keys: {self.core.reqId_hashmap.keys()}')
                #self.core.reqId_hashmap[reqId].__self__.set_error_flag(flag=True)
            #except KeyError:
                #tprint('Passed')
                #pass

    def build_connection(self):
        print(123, self.core.connection_status.value)
        while self.core.connection_status == ConnectionStatus.DISCONNECTED:
            try:
                print(1)
                self.connect(self.core.host_ip, self.core.api_port, self.core.client_id)
                print(2)
                time.sleep(2)
                if self.isConnected():
                    print(3)
                    break
            except Exception as err:
                print(4, self.isConnected(), err)
                time.sleep(5)


    def historicalData(self, reqId, bar):
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

