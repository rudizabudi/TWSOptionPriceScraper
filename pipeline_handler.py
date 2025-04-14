from datetime import datetime, timedelta
from math import floor, ceil
from threading import Thread
from time import sleep
import traceback

from contract_container import ContractContainer
from core import Core, EnvDistributor, tprint
from database_broker import DatabaseBroker


class PipelineHandler:

    def __init__(self):

        self.core: Core = EnvDistributor.get_core()
        self.db: DatabaseBroker = DatabaseBroker()
        self.ContractContainer: type[ContractContainer] = ContractContainer

        self.t1 = Thread(target=self.request_prices).start()
        self.t2 = Thread(target=self.write_to_database).start()

        self.tws_con = self.core.tws_con

        self.last_write_time: datetime | None = None

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

        tprint('Requesting prices...')
        while True:
            try:
                contract_instance = self.core.immediate_pool[0]
                last_update = contract_instance.get_last_update()
                last_update = last_update if last_update else datetime(year=datetime.today().year - 2, month=1, day=1)

                duration = max(floor((datetime.now() - last_update) / timedelta(days=7) + 1), 1)

                if duration > 52:
                    duration_str = f'{ceil(duration / 52)} Y'
                elif duration <= 52:
                    duration_str = f'{duration} W'
                else:
                    raise Exception(f'Invalid duration: {duration}')

                contract_instance.set_reqId_assign(self.core.reqId_2, reqType='reqHistData')
                query_time = datetime.today().strftime("%Y%m%d-%H:%M:%S")

                self.core.last_request = datetime.now()
                self.tws_con.reqHistoricalData(reqId=self.core.reqId_2,
                                               contract=contract_instance.get_contract(),
                                               endDateTime=query_time,
                                               durationStr=duration_str,
                                               barSizeSetting=self.core.CANDLE_LENGTH,
                                               whatToShow="Bid_Ask",
                                               useRTH=1,
                                               formatDate=1,
                                               keepUpToDate=False,
                                               chartOptions=[])

                self.core.reqId_2 += 1
                timeout_secs = 60
                for k in self.core.timeout_breaker.keys():
                    if duration <= k:
                        timeout_secs = self.core.timeout_breaker[k]
                time_breaker = datetime.now() + timedelta(seconds=timeout_secs)
                while not contract_instance.get_error_flag() and not contract_instance.get_historical_data_end() and datetime.now() < time_breaker:
                    if not self.core.tws_con.isConnected():
                        while not self.tws_con.isConnected():
                            self.tws_con = self.core.tws_con
                            sleep(10)
                        break
                    sleep(.1)
                if contract_instance.get_historical_data_end():
                    self.core.writable_pool.append(contract_instance)

                self.core.immediate_pool.pop(0)

            except IndexError:
                while len(self.core.immediate_pool) == 0:
                    sleep(.1)
            except Exception as e:
                tprint(f'Unhandled exception: {e} {traceback.format_exc()}', debug=True)

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
                for i, (dt, ohlc) in enumerate(contract_instance.get_price_data().items(), start=1):
                    dt_dt = datetime.strptime(dt, '%Y%m%d %H:%M:%S')
                    if not existing_dates or dt_dt not in existing_dates:
                        if (dt_dt.year, dt_dt.month, dt_dt.day) not in self.core.utc_diffs.keys():
                            new_start_range = datetime.today() - datetime(dt_dt.year, dt_dt.month, dt_dt.day)
                            self.core.create_time_offset_table(start_range=-1 * (new_start_range.days + 30))

                        time_offset = self.core.NORMALIZED_TIME_DIFF + self.core.utc_diffs[dt_dt.year, dt_dt.month, dt_dt.day]

                        dt_dt += timedelta(hours=time_offset)
                        dt = dt_dt.strftime('%Y%m%d %H:%M:%S')

                        match contract_instance.get_secType():
                            case 'STK':
                                data_query = f"('{dt}', {ohlc['High']}, {ohlc['Low']}, {ohlc['Open']}, {ohlc['Close']})"
                            case 'OPT':
                                security_identifier = f'{contract_instance.get_symbol()}_{contract_instance.get_strike()}_{contract_instance.get_right()}_{contract_instance.get_expiry(output_str_format='%d%b%y')}'

                                data_query = f"('{dt}', '{security_identifier}', '{contract_instance.get_right()}', {contract_instance.get_strike()}, {ohlc['High']}, {ohlc['Low']}, {ohlc['Open']}, {ohlc['Close']})"
                            case _:
                                raise Exception(f'Invalid secType: {contract_instance.get_secType()}')

                        iq_rows.append(data_query)

                if iq_header and iq_rows:
                    if contract_instance.get_secType() == 'OPT':
                        tprint(f'Writing {len(iq_rows):>5} new price data points for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()} {contract_instance.get_right()} {contract_instance.get_strike()}.')
                    else:
                        tprint(f'Writing {len(iq_rows):>5} new price data points for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()}.')

                    for i in range(ceil(len(iq_rows) / self.core.INSERT_QUERY_MAX_LINES)):
                        insert_query = iq_header + ','.join(str(x) for x in iq_rows[i * self.core.INSERT_QUERY_MAX_LINES:min(len(iq_rows), (i + 1) * self.core.INSERT_QUERY_MAX_LINES)]) + ';'
                        self.db.write_price_data(query_string=insert_query)
                else:
                    if contract_instance.get_secType() == 'OPT':
                        tprint(f'Writing no new price data points for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()} {contract_instance.get_right()} {contract_instance.get_strike()}.')
                    else:
                        tprint(f'Writing no new price data points for {contract_instance.get_symbol()} {contract_instance.get_secType()} to database {contract_instance.get_table()}.')

                self.core.writable_pool.pop(0)

            except IndexError:
                while len(self.core.writable_pool) == 0:
                    sleep(.1)
