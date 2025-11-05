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
                while len(self.core.immediate_pool) == 0:
                    sleep(0.1)

                contract = self.core.immediate_pool[0]
                last_update = self.db.get_last_update(contract_container=contract)
                last_update = last_update if last_update else datetime(year=datetime.today().year - 2, month=1, day=1)

                duration = max(floor((datetime.now() - last_update) / timedelta(days=7) + 1), 1)

                if duration > 52:
                    duration_str = f'{min(ceil(duration / 52), 2)} Y'
                elif duration <= 52:
                    duration_str = f'{duration} W'
                else:
                    raise Exception(f'Invalid duration: {duration}')

                contract.set_reqId_assign(self.core.reqId_2, reqType='reqHistData')
                query_time = datetime.today().strftime("%Y%m%d-%H:%M:%S")

                self.core.last_request = datetime.now()
                self.tws_con.reqHistoricalData(reqId=self.core.reqId_2,
                                               contract=contract.get_contract(),
                                               endDateTime=query_time,
                                               durationStr=duration_str,
                                               barSizeSetting=self.core.CANDLE_LENGTH,
                                               whatToShow='Bid_Ask',
                                               useRTH=1,
                                               formatDate=1,
                                               keepUpToDate=False,
                                               chartOptions=[])

                self.core.reqId_2 += 1
                tprint(f'Requested: {contract}: {last_update=}, {duration=}', debug=True)

                timeout_secs = 60
                for k in self.core.timeout_breaker.keys():
                    if duration <= k:
                        timeout_secs = self.core.timeout_breaker[k]

                time_breaker = datetime.now() + timedelta(seconds=timeout_secs)
                while not contract.get_error_flag() and not contract.get_historical_data_end() and datetime.now() < time_breaker:
                    if not self.core.tws_con.isConnected():
                        while not self.tws_con.isConnected():
                            sleep(10)
                            self.tws_con = self.core.tws_con
                        break
                    sleep(.1)

                if contract.get_historical_data_end():
                    self.core.writable_pool.append(contract)
                self.core.immediate_pool.pop(0)

            except IndexError:
                while len(self.core.immediate_pool) == 0:
                    sleep(.1)
            # except Exception as e:
            #     tprint(f'Unhandled exception: {e} {traceback.format_exc()}', debug=True)

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
                while len(self.core.writable_pool) == 0:
                    sleep(0.1)

                contract_instance = self.core.writable_pool[0]
                
                existing_dates = self.db.get_existing_dates(contract_container=contract_instance)

                match contract_instance.get_secType():
                    case 'STK':
                        columns = 'date, h, l, o, c'
                    case 'OPT':
                        columns = 'date, identifier, callput, strike, h, l, o, c'
                    case _:
                        raise Exception(f'Invalid secType: {contract_instance.get_secType()}')

                iq_header = self.db.gen_insert_header(contract=contract_instance, columns=columns)

                #Normalize requested price data for time offset
                requested_pricing = {}
                for dt, ohlc in contract_instance.get_price_data().items():
                    timestamp = datetime.strptime(dt, '%Y%m%d %H:%M:%S')

                    if (timestamp.year, timestamp.month, timestamp.day) not in self.core.utc_diffs.keys():
                        new_start_range = datetime.today() - datetime(timestamp.year, timestamp.month, timestamp.day)
                        self.core.create_time_offset_table(start_range=-1 * (new_start_range.days + 30))

                    time_offset = self.core.NORMALIZED_TIME_DIFF + self.core.utc_diffs[timestamp.year, timestamp.month, timestamp.day]

                    timestamp += timedelta(hours=time_offset)
                    requested_pricing[timestamp] = ohlc

                iq_rows = []
                for i, (dt, ohlc) in enumerate(requested_pricing.items(), start=1):
                    if not existing_dates or dt not in existing_dates:
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
                        contract = f'{contract_instance.get_symbol()} {contract_instance.get_secType()}'
                        database = f'{contract_instance.get_table()} {contract_instance.get_right()} {contract_instance.get_strike()}'
                        tprint(f'Writing {len(iq_rows):>5} new price data points for {contract:>10}{' ':>3} to database {database:>30}.')
                    else:
                        contract = f'{contract_instance.get_symbol()} {contract_instance.get_secType()}'
                        tprint(f'Writing {len(iq_rows):>5} new price data points for {contract:>10}{' ':>3} to database {contract_instance.get_table():>30}.')

                    for i in range(ceil(len(iq_rows) / self.core.INSERT_QUERY_MAX_LINES)):
                        insert_query = iq_header + ','.join(str(x) for x in iq_rows[i * self.core.INSERT_QUERY_MAX_LINES:min(len(iq_rows), (i + 1) * self.core.INSERT_QUERY_MAX_LINES)]) + ';'
                        self.db.write_price_data(query_string=insert_query,
                                                 database=contract_instance.get_database())
                else:
                    if contract_instance.get_secType() == 'OPT':
                        contract = f'{contract_instance.get_symbol()} {contract_instance.get_secType()}'
                        database = f'{contract_instance.get_table()} {contract_instance.get_right()} {contract_instance.get_strike()}'
                        tprint(f'Writing {'no':>5} new price data points for {contract:>10}{' ':>3} to database {database:>30}.')
                    else:
                        contract = f'{contract_instance.get_symbol()} {contract_instance.get_secType()}'
                        tprint(f'Writing {'no':>5} new price data points for {contract:>10}{' ':>3} to database {contract_instance.get_table():>30}.')

                self.core.writable_pool.pop(0)

            except IndexError:
                while len(self.core.writable_pool) == 0:
                    sleep(.1)

            except Exception as e:
                print(Exception(f'Unhandled exception {e}.'))
                self.core.writable_pool.pop(0)
