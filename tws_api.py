from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from threading import Thread, Event, current_thread

import time

from core import tprint, ConnectionStatus


class TWSCon(EWrapper, EClient):

    def __init__(self, core):
        super().__init__()
        EClient.__init__(self, wrapper=self)

        self.core = core
        self.core.no_contract = False

        self.reconnecting: bool = False

        self.t: Thread | None = None
        self.thread_ready: Event = Event()
        self.build_connection()

    def get_thread(self):
        self.thread_ready.wait()
        return self.t

    def connectAck(self):
        tprint(f'Connected to TWS API. {self.isConnected()}')
        self.core.connection_status = ConnectionStatus.CONNECTED
        self.reconnecting = False

    def connectionClosed(self):
        time.sleep(10)
        if not self.isConnected() and not self.reconnecting:
            tprint('Disconnected from TWS API.')
            self.core.connection_status = ConnectionStatus.DISCONNECTED

            if self.t and not self.reconnecting:
                self.reconnecting = True
                self.reconnect()

    def error(self, reqId, errorCode, errorString):
        tprint(f'Error: {errorCode} --> {errorString}', debug=True)
        if errorCode in [162, 200]:
            self.core.reqId_hashmap[reqId].__self__.set_error_flag(flag=True)

        if errorCode == 504:
            tprint(f'Error thread: {self.get_instance_info(self.t)}, {self.isConnected()}', debug=True)

    def build_connection(self):
        # try:
        #     if self.t:
        #         self.reconnecting = True
        #         self.t.join()
        #         tprint('Thread joined.', debug=True)
        # except Exception as err:
        #     print(err)

        while self.core.connection_status == ConnectionStatus.DISCONNECTED:
            self.connect(self.core.host_ip, self.core.api_port, self.core.client_id)
            time.sleep(2)
            self.t: Thread = Thread(target=self.run)
            self.t.start()
            time.sleep(2)
            #self.thread_ready.set()

            if self.isConnected():
                break

    def reconnect(self):
        while not self.isConnected():
            tprint('Reconnecting...')
            self.connect(self.core.host_ip, self.core.api_port, self.core.client_id)
            time.sleep(2)

        tprint(f'Reconnected successfully.', debug=True)
        old_t = self.t
        tprint(f'Old_t1: {self.get_instance_info(old_t)}, {self.isConnected()}', debug=True)
        time.sleep(2)
        # self.t: Thread = Thread(target=self.run)
        # self.t.start()
        # time.sleep(2)
        tprint(f'Old_t2: {self.get_instance_info(old_t)}', debug=True)
        tprint(f'New_t: {self.get_instance_info(self.t)}, {self.isConnected()}', debug=True)
        tprint(f'Current API Class object: {self}', debug=True)

        #self.thread_ready.set()
        self.reconnecting = False
        tprint(f'Reconnecting set False', debug=True)
        time.sleep(5)
        tprint(f'New_t2: {self.get_instance_info(self.t)}, {self.isConnected()}', debug=True)


    def get_instance_info(self, t=None):
        if not t:
            t = self.t

        info = {'thread_name': t.name,
                'connection_value': self.core.connection_status.value,
                'thread_id': t.ident,
                'thread_alive': t.is_alive()}
        return info

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

# @deprecated
def connection_loop(core):
    tws_thread: Thread | None = None
    while True:
        if core.connection_status == ConnectionStatus.DISCONNECTED:
            tprint('Connection loop condition met. ' + str(tws_thread))
            if tws_thread:
                time.sleep(5)
                tprint('Old thread to be joined.')
                tws_thread.join()
                tprint('Connection loop old thread joined.')

            core.tws_con = TWSCon(core=core)
            tws_thread = core.tws_con.get_thread()
            tprint(f'Connection thread started.' + str(tws_thread))

        time.sleep(10)


