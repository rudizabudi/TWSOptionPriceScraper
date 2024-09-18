import threading
from datetime import datetime

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

import time

class TWSCon(EWrapper, EClient):

    def __init__(self, core):
        super().__init__()
        EClient.__init__(self, wrapper=self)

        self.core = core
        self.core.no_contract = False

        self.connect(core.host_ip, core.api_port, core.client_id)
        self.t: threading.Thread = threading.Thread(target=self.run, daemon=True)
        self.t.start()
        time.sleep(1)

    def connectAck(self):
        print(datetime.now().strftime('%H:%M:%S') + ' : ', 'Connected.')

    def connectionClosed(self):
        print(datetime.now().strftime('%H:%M:%S') + ' : ', 'Disconnected.')

    def error(self, reqId, errorCode, errorString):
        # print(errorCode, errorString)
        if errorCode in [162, 200]:
            self.core.response_state = -1

    def historicalData(self, reqId, bar):
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId]({bar.date: {'Open': bar.open, 'High': bar.high, 'Low': bar.low, 'Close': bar.close}})


    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        self.hde = True
        # print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)

    def historicalTicksBidAsk(self, reqId, ticks, done):
        print('Ticks:', ticks)

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes):

        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](expiries=list(expirations) or [], strikes=list(strikes) or [])
        self.core.response_state = 1


        # if reqId not in self.reqId_cache.keys():
        #     self.reqId_cache[reqId] = {'expiries': [],
        #                                'strikes': []}
        # for x in list(expirations):
        #     if x not in self.reqId_cache[reqId]['expiries']:
        #         self.reqId_cache[reqId]['expiries'].append(x)
        #
        # for x in list(strikes):
        #     if x not in self.reqId_cache[reqId]['strikes']:
        #         self.reqId_cache[reqId]['strikes'].append(x)

    def contractDetails(self, reqId: int, contractDetails):
        if reqId not in self.core.reqId_hashmap.keys():
            raise KeyError('ReqId not assigned to an security class instance.')

        self.core.reqId_hashmap[reqId](contractDetails.contract.conId)
        self.core.response_state = 1

