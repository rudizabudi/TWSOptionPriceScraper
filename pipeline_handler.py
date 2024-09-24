from datetime import datetime, timedelta
from math import floor, ceil
from threading import Thread
from time import sleep

from core import tprint

import traceback

class PipelineHandler:
    def __init__(self, core =None, tws_con=None, CC=None, DB=None):
        if None in (core, tws_con, CC, DB):
            raise Exception('<PipelineBuilder INIT> All parameters must be specified.')

        self.core = core
        self.tws_con = tws_con
        self.ContractContainer = CC
        self.db = DB

        self.t1 = Thread(target=self.request_prices, daemon=True).start()
        self.t2 = Thread(target=self.write_to_database, daemon=True).start()


    def request_prices(self):
        """
            Requests historical price data for the contracts in the immediate pool.

            This method retrieves the contracts from the immediate pool and requests
            their historical price data using the TWS API's reqHistoricalData method.
            Request parameters are determined by present data, e.g. last_update time.
            The method waits until the data is available or a timeout is reached.
            The retrieved data is added to the writable pool.

            :input: self.core.immediate_pool :popping
            :output: self.core.writable_pool :appending
            """
        # print("Request_prices called from:")
        # for line in traceback.format_stack()[:-1]:
        #     print(line.strip())

        while not self.core.immediate_pool:
            sleep(10)

        while True:
            try:
                contract_instance = self.core.immediate_pool[0]
                last_update = contract_instance.get_last_update()
                last_update = last_update if last_update else datetime(year=datetime.today().year - 2, month=1, day=1)

                duration = max(floor((datetime.now() - last_update) / timedelta(days=7) + 1), 1)

                if duration > 52:
                    durationStr = f'{ceil(duration / 52)} Y'
                elif duration <= 52:
                    durationStr = f'{duration} W'
                else:
                    raise Exception(f'Invalid duration: {duration}')

                #tprint(f'Requesting prices for {contract_instance.get_symbol()} with last update {last_update} and duration {durationStr}')
                contract_instance.set_reqId_assign(self.core.reqId_2, reqType='ReqHistData')
                query_time = datetime.today().strftime("%Y%m%d-%H:%M:%S")
                self.tws_con.reqHistoricalData( reqId=self.core.reqId_2,
                                                contract=contract_instance.get_contract(),
                                                endDateTime=query_time,
                                                durationStr=durationStr,
                                                barSizeSetting=self.core.candle_length,
                                                whatToShow="Bid_Ask",
                                                useRTH=1,
                                                formatDate=1,
                                                keepUpToDate=False,
                                                chartOptions=[])
                self.core.reqId_2 += 1

                timeout_secs = 60
                for k in self.core.timeout_breaker.keys():
                    if duration <= k: timeout_secs = self.core.timeout_breaker[k]
                time_breaker = datetime.now() + timedelta(seconds=timeout_secs)

                while not contract_instance.get_error_flag() and not contract_instance.get_historical_data_end() and datetime.now() < time_breaker:
                    if datetime.now() >= time_breaker and self.tws_con.isConnected():
                        #print(self.now_print(), 'ReqHistoricalData TimeBreaker.')
                        # TODO: reschedule contract a couple of positions later
                        sleep(.1)
                        break
                    self.connection_handler()
                    pass

                if contract_instance.get_historical_data_end():
                    self.core.writable_pool.append(contract_instance)

                self.core.immediate_pool.pop(0)

            except IndexError:
                while len(self.core.immediate_pool) == 0:
                    sleep(.1)

    def write_to_database(self):
        """
            Writes price data from the writable pool to the database.

            This method continuously checks the writable pool for contract instances
            with price data to be written to the database. It generates and passes on an INSERT query
            for each contract instance and executes it to write the data to the database.

            :input: self.core.writable_pool :popping
            :output: self.db SQL class :pushing
            """
        while not self.core.writable_pool:
            sleep(10)

        self.db = self.db(core=self.core, CC=self.ContractContainer)
        while True:
            try:
                contract_instance = self.core.writable_pool[0]
                existing_dates = self.db.get_existing_dates(contract_container=contract_instance)

                match contract_instance.get_secType():
                    case 'STK':
                        columns = 'date, h, l, o, c'
                    case 'OPT':
                        columns = 'date, identifier, callput, strike, h, l, o, c'
                    case _:
                        raise Exception(f'Invalid secType: {contract_instance.get_secType()}')

                iq_header = f"""
                            INSERT INTO [{contract_instance.get_database()}].[dbo].[{contract_instance.get_table()}] ({columns})
                            VALUES
                            """

                iq_rows = []
                # print(contract_instance.get_table(), existing_dates)
                for i, (dt, ohlc) in enumerate(contract_instance.get_price_data().items(), start=1):
                    #TODO: Check if dt is already datetime object
                    if not existing_dates or datetime.strptime(dt, "%Y%m%d %H:%M:%S") not in existing_dates:
                        match contract_instance.get_secType():
                            case 'STK':
                                data_query = f"('{dt}', {ohlc['High']}, {ohlc['Low']}, {ohlc['Open']}, {ohlc['Close']})"
                            case 'OPT':
                                security_identifier = f'{contract_instance.get_expiry(output_str_format='%d%b%y')}_{contract_instance.get_symbol()}_{contract_instance.get_strike()}_{contract_instance.get_right()}'

                                data_query = f"('{dt}', '{security_identifier}', '{contract_instance.get_right()}', {contract_instance.get_strike()}, {ohlc['High']}, {ohlc['Low']}, {ohlc['Open']}, {ohlc['Close']})"
                            case _:
                                raise Exception(f'Invalid secType: {contract_instance.get_secType()}')

                        iq_rows.append(data_query)

                if iq_header and iq_rows:
                    if contract_instance.get_secType() == 'OPT':
                        tprint(f'Writing #{len(iq_rows)} price data for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()} {contract_instance.get_right()} {contract_instance.get_strike()}.')
                    else:
                        tprint(f'Writing #{len(iq_rows)} price data for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()}.')

                    for i in range(ceil(len(iq_rows) / self.core.insert_query_max_lines)):
                        insert_query = iq_header + ','.join(str(x) for x in iq_rows[i * self.core.insert_query_max_lines:min(len(iq_rows), (i + 1) * self.core.insert_query_max_lines)]) + ';'
                        self.db.write_price_data(query_string=insert_query)

                self.core.writable_pool.pop(0)

            except IndexError as err:
                #tprint(f'IndexError 987 {err}')
                while len(self.core.writable_pool) == 0:
                    sleep(.1)


    def connection_handler(self) -> bool:
        if not self.tws_con.isConnected():
            print('Disconnected 3456')
            while True:
                print(1)
                self.tws_con.connect(self.core.host_ip, self.core.api_port, self.core.client_id)
                print(2)
                sleep(10)
                if self.tws_con.isConnected():
                    print(3)
                    break
        return True
