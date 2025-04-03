from ast import literal_eval
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import json
import os
import psutil
import pytz
from typing import Callable

load_dotenv('.env')

DEBUG_MODE: bool = False


class Core:
    def __init__(self):
        if not os.path.exists(os.path.join(os.path.dirname(__file__), '.env')):
            raise Exception('.env file not found. Please fill and rename .env_rename')

        #  General Settings:
        self.CANDLE_LENGTH: str = os.getenv('CANDLE_LENGTH')
        self.RANDOMIZE_OPTS: bool = literal_eval(os.getenv('RANDOMIZE_OPTS'))
        self.STK_UPDATE_TIME: list[int] = literal_eval(os.getenv('STK_UPDATE_TIME'))  # list[hour, minute]
        self.EXP_UPDATE_TIME: list[int] = literal_eval(os.getenv('EXP_UPDATE_TIME'))  # list[hour, minute]

        # Constituents list updater
        self.GRACE_PERIOD: int = int(os.getenv('GRACE_PERIOD'))  # grace period in days after STK left index
        self.UPDATE_CSV_PATH: str = os.getenv('UPDATE_CSV_PATH')
        self.EXTRA_SYMBOLS: list[str] = os.getenv('EXTRA_SYMBOLS').split(',')

        # TWS API credentials
        self.HOST_IP: str = os.getenv('HOST_IP')
        self.API_PORT: int = int(os.getenv('API_PORT'))
        self.CLIENT_ID: int = int(os.getenv('CLIENT_ID'))

        # Microsoft SQL Server credentials
        self.SQL_SERVER: str = os.getenv('SQL_SERVER')
        self.SQL_USER: str = os.getenv('SQL_USER')
        self.SQL_PASSWORD: str = os.getenv('SQL_PASSWORD')
        self.SQL_CONNECTION_STRING: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SQL_SERVER};UID={self.SQL_USER};PWD={self.SQL_PASSWORD}'

        # IBC settings for TWS API restart
        self.USE_IBC: bool = bool(os.getenv('USE_IBC'))
        self.STARTGW_IBC_PATH: str = os.getenv('START_GW_PATH')

        self.validate_env_input()

        # Further user defined settings
        self.IP_LENGTH: int = 10  # length for immediate_pool length. Queue between pipeline_builder and pipeline_handler
        self.TIMER_EXCLUDE_DAYS: list[int] = [5, 6]  # skip trading day triggers on these weekdays (0-6)
        self.INSERT_QUERY_MAX_LINES: int = 995  # max amount of inserts per query
        self.GLITCH_DETECTOR_THRESHOLD: int = 900  # threshold in seconds after which tws_api glitching is assumed
        self.EXPIRED_OPT_DAYS = 2  # threshold in days after that an option is considered expired (inclusive)

        self.LOCAL_TZ: str = 'Europe/Berlin'  # name of local timezone
        self.EXCHANGE_TZ: str = 'America/New_York'  # name of exchange timezone
        self.NORMALIZED_TIME_DIFF: int = 6  # usual time difference between local and exchange

        self.RESTART_THRESHOLD: int = 300  # if automatic API restart fails try again in secs

        self.EXP_OPT_FILE_NAME: str = 'expired_option_contracts.pkl'
        self.JSON_SESSION_FILE_NAME: str = 'session_data.json'

        # Initialization of shared variable space.
        self.reqId_hashmap: dict[int: Callable] = {}
        self.reqId_1: int = 1
        self.reqId_2: int = 100_000_000

        self.underlying_list: dict[str, list[str]] = {'STK': []}

        self.contract_pool: dict[str, list[object]] = {'STK': [],
                                                       'OPT': [],
                                                       'EXP': []}

        self.immediate_pool: list["ContractContainer"] = []

        self.writable_pool: list["ContractContainer"] = []

        self.timeout_breaker: dict[int, int] = {4: 20, 8: 40, 26: 120, 52: 180, 9999: 300}

        # Pipeline time triggers
        self.stk_last_update: datetime = datetime.fromtimestamp(read_data_json(self).get('STK_LAST_UPDATE', 0))
        self.exp_last_update: datetime = datetime.fromtimestamp(read_data_json(self).get('EXP_LAST_UPDATE', 0))

        self.stk_update_timer: datetime = datetime.today().replace(hour=self.STK_UPDATE_TIME[0], minute=self.STK_UPDATE_TIME[1], second=0, microsecond=0)
        self.exp_update_timer: datetime = datetime.today().replace(hour=self.EXP_UPDATE_TIME[0], minute=self.EXP_UPDATE_TIME[1], second=0, microsecond=0)
        self.monday_roll_timer: datetime = next(filter(lambda x: x.weekday() == 0, ((datetime.today() + timedelta(days=x + 1) for x in range(0, 7))))).replace(hour=6, minute=0)

        self.startup: bool = True
        self.tws_con: "TWSCon" = None

        self.last_request: datetime = None
        self.last_receive: datetime = None

        self.time_disconnect: datetime = None

        self.utc_diffs: dict[tuple[int]: int] = {}
        self.create_time_offset_table()

    def validate_env_input(self):
        if self.CANDLE_LENGTH not in ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs', '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours', '1 day', '1W', '1M'):
            raise ValueError(f'Illegal candle length: {self.CANDLE_LENGTH}')
        if not isinstance(self.RANDOMIZE_OPTS, bool):
            raise ValueError(f'Illegal value for RANDOMIZE_OPTS: {self.RANDOMIZE_OPTS}')
        if not all(map(lambda x: isinstance(x, int), self.STK_UPDATE_TIME)) or not len(self.STK_UPDATE_TIME) == 2:
            raise ValueError(f'Illegal value for STK_UPDATE_TIME: {self.STK_UPDATE_TIME}')
        if not all(map(lambda x: isinstance(x, int), self.EXP_UPDATE_TIME)) or not len(self.EXP_UPDATE_TIME) == 2:
            raise ValueError(f'Illegal value for EXP_UPDATE_TIME: {self.EXP_UPDATE_TIME}')

        if not len(self.HOST_IP.split('.')) == 4 and all(map(lambda x: isinstance(x, int), self.HOST_IP.split('.'))):
            raise ValueError(f'Illegal value for HOST_IP: {self.HOST_IP}')
        if not isinstance(self.API_PORT, int):
            raise ValueError(f'Illegal value for API_PORT: {self.API_PORT}')
        if not isinstance(self.CLIENT_ID, int):
            raise ValueError(f'Illegal value for CLIENT_ID: {self.CLIENT_ID}')

        if not self.SQL_SERVER or not self.SQL_USER or not self.SQL_PASSWORD:
            raise ValueError(f'Illegal value for SQL_SERVER, SQL_USER, SQL_PASSWORD: {self.SQL_SERVER}, {self.SQL_USER}, {self.SQL_PASSWORD}')

        if not isinstance(self.GRACE_PERIOD, int):
            raise ValueError(f'Illegal value for GRACE_PERIOD: {self.GRACE_PERIOD}')
        if not isinstance(self.UPDATE_CSV_PATH, str):
            raise ValueError(f'Illegal value for UPDATE_CSV_PATH: {self.UPDATE_CSV_PATH}')

        if not isinstance(self.USE_IBC, bool):
            raise ValueError(f'Illegal value for USE_IBC: {self.USE_IBC}')
        if not isinstance(self.STARTGW_IBC_PATH, str):
            raise ValueError(f'Illegal value for START_GW_PATH: {self.STARTGW_IBC_PATH}')

    def write_tws_connection(self, TWSCon):
        self.tws_con = TWSCon

    def create_time_offset_table(self):
        for day_dif in range(-30, 365):
            date = datetime.now(timezone.utc) - timedelta(days=day_dif)

            local_time = date.astimezone(pytz.timezone(self.LOCAL_TZ))
            trade_time = date.astimezone(pytz.timezone(self.EXCHANGE_TZ))
            self.utc_diffs[date.year, date.month, date.day] = (trade_time.utcoffset() - local_time.utcoffset()).total_seconds() / 60 / 60


def tprint(text: str = '', *args, debug: bool = False, **kwargs):
    if (debug and DEBUG_MODE) or not debug:
        print(f'{datetime.now().strftime('%H:%M:%S')} : {text}')


def kill_ibgateway():
    pids = {psutil.Process(x).name(): x for x in psutil.pids()}

    if pid := pids.get('ibgateway.exe'):
        psutil.Process(pid).kill()
        tprint(f'IBGateway closed.')
    else:
        tprint('IBGateway process not found.')


def start_ibgateway(core):
    if os.path.exists(core.STARTGW_IBC_PATH):
        os.system(core.STARTGW_IBC_PATH)
    else:
        raise Exception(f'IBC not found. Provided path: {core.STARTGW_IBC_PATH}')

    tprint(f'IBGateway started.')


def read_data_json(core) -> dict[str: str | float] | defaultdict:
    if os.path.exists(os.path.join(os.path.dirname(__file__), core.JSON_SESSION_FILE_NAME)):
        with open(core.JSON_SESSION_FILE_NAME, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
        return loaded_data

    return defaultdict(dict)


def write_data_json(core, data: dict = None):
    if not isinstance(data, dict):
        raise TypeError('Json data must be a dictionary.')

    loaded_data = read_data_json(core)
    for k, v in data.items():
        loaded_data[k] = v

    with open(core.JSON_SESSION_FILE_NAME, 'w', encoding='utf-8') as f:
        json.dump(loaded_data, f, indent=4)




