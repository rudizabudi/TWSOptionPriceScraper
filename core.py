from ast import literal_eval
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from Exscript.protocols import Telnet
import json
import os
import psutil
import pytz
from time import sleep
from typing import Callable

load_dotenv('.env')

DEBUG_MODE: bool = False


class EnvDistributor:
    """
    Save Core instance as class variable to be accessed via imports-> cls.method getter
    """
    core = None

    @classmethod
    def set_core(cls, core):
        cls.core = core

    @classmethod
    def get_core(cls):
        if not isinstance(cls.core, Core):
            raise Exception('Core not set.')
        return cls.core


class Core:
    #  General Settings:
    CANDLE_LENGTH: str = os.getenv('CANDLE_LENGTH')
    RANDOMIZE_OPTS: bool = literal_eval(os.getenv('RANDOMIZE_OPTS'))
    STK_UPDATE_TIME: list[int] = literal_eval(os.getenv('STK_UPDATE_TIME'))  # list[hour, minute]
    EXP_UPDATE_TIME: list[int] = literal_eval(os.getenv('EXP_UPDATE_TIME'))  # list[hour, minute]

    # Constituents list updater
    GRACE_PERIOD: int = int(os.getenv('GRACE_PERIOD'))  # grace period in days after STK left index
    UPDATE_CSV_PATH: str = os.getenv('UPDATE_CSV_PATH')
    EXTRA_SYMBOLS: list[str] = os.getenv('EXTRA_SYMBOLS').split(',')

    # TWS API credentials
    HOST_IP: str = os.getenv('HOST_IP')
    API_PORT: int = int(os.getenv('API_PORT'))
    CLIENT_ID: int = int(os.getenv('CLIENT_ID'))

    # Microsoft SQL Server credentials
    SQL_SERVER: str = os.getenv('SQL_SERVER')
    SQL_USER: str = os.getenv('SQL_USER')
    SQL_PASSWORD: str = os.getenv('SQL_PASSWORD')
    SQL_CONNECTION_STRING: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};UID={SQL_USER};PWD={SQL_PASSWORD}'

    # IBC settings for TWS API restart
    USE_IBC: bool = bool(os.getenv('USE_IBC'))
    STARTGW_IBC_PATH: str = os.getenv('START_GW_PATH')
    IBC_TELNET_IP: str = os.getenv('IBC_TELNET_IP')
    IBC_TELNET_PORT: int = int(os.getenv('IBC_TELNET_PORT'))
    GATEWAY_PROCESS_NAME: str = 'java.exe'

    # Further user defined settings
    IP_LENGTH: int = 10  # length for immediate_pool length. Queue between pipeline_builder and pipeline_handler
    TIMER_EXCLUDE_DAYS: list[int] = [5, 6]  # skip trading day triggers on these weekdays (0-6)
    INSERT_QUERY_MAX_LINES: int = 995  # max amount of inserts per query
    GLITCH_DETECTOR_THRESHOLD: int = 900  # threshold in seconds after which tws_api glitching is assumed
    EXPIRED_OPT_DAYS: int = 2  # threshold in days after that an option is considered expired (inclusive)
    OPT_LIST_CURRENT: int = 7  # days after which creation the option list is considered current

    LOCAL_TZ: str = 'Europe/Berlin'  # name of local timezone
    EXCHANGE_TZ: str = 'America/New_York'  # name of exchange timezone
    NORMALIZED_TIME_DIFF: int = 6  # usual time difference between local and exchange

    RESTART_THRESHOLD: int = 300  # if automatic API restart fails try again in secs

    MAIN_OPT_FILE_NAME: str = 'main_option_contracts.pkl'
    EXP_OPT_FILE_NAME: str = 'expired_option_contracts.pkl'
    JSON_SESSION_FILE_NAME: str = 'session_data.json'

    # Initialization of shared variable space.
    reqId_hashmap: dict[int: Callable] = {}
    reqId_1: int = 1
    reqId_2: int = 100_000_000

    underlying_list: dict[str, list[str]] = {'STK': []}

    contract_pool: dict[str, list['ContractContainer']] = {'STK': [], 'OPT': [], 'EXP': []}

    immediate_pool: list['ContractContainer'] = []

    writable_pool: list['ContractContainer'] = []

    timeout_breaker: dict[int: int] = {4: 20, 8: 40, 26: 120, 52: 180, 9999: 300}

    stk_update_timer: datetime = datetime.today().replace(hour=STK_UPDATE_TIME[0], minute=STK_UPDATE_TIME[1], second=0, microsecond=0)
    exp_update_timer: datetime = datetime.today().replace(hour=EXP_UPDATE_TIME[0], minute=EXP_UPDATE_TIME[1], second=0, microsecond=0)
    monday_roll_timer: datetime = next(filter(lambda x: x.weekday() == 0, ((datetime.today() + timedelta(days=x + 1) for x in range(0, 7))))).replace(hour=6, minute=0)

    startup: bool = True
    tws_con: 'TWSCon' = None

    last_request: datetime = None
    last_receive: datetime = None

    time_disconnect: datetime = None

    utc_diffs: dict[tuple[int]: int] = {}

    def __init__(self):
        if not os.path.exists(os.path.join(os.path.dirname(__file__), '.env')):
            raise Exception('.env file not found. Please fill and rename .env_rename')

        self.validate_env_input()

        self.create_time_offset_table()

        # Pipeline time triggers
        self.stk_last_update: datetime = datetime.fromtimestamp(read_data_json(self).get('STK_LAST_UPDATE', 0))
        self.exp_last_update: datetime = datetime.fromtimestamp(read_data_json(self).get('EXP_LAST_UPDATE', 0))
        self.last_opt_build: datetime = datetime.fromtimestamp(read_data_json(self).get('LAST_OPT_BUILD', 0))

        EnvDistributor.set_core(self)

    def validate_env_input(self):
        if self.CANDLE_LENGTH not in ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs', '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours', '1 day', '1W', '1M'):
            raise ValueError(f'Illegal candle length: {self.CANDLE_LENGTH}')
        if not isinstance(self.RANDOMIZE_OPTS, bool):
            raise ValueError(f'Invalid value for RANDOMIZE_OPTS: {self.RANDOMIZE_OPTS}')
        if not all(map(lambda x: isinstance(x, int), self.STK_UPDATE_TIME)) or not len(self.STK_UPDATE_TIME) == 2:
            raise ValueError(f'Invalid value for STK_UPDATE_TIME: {self.STK_UPDATE_TIME}')
        if not all(map(lambda x: isinstance(x, int), self.EXP_UPDATE_TIME)) or not len(self.EXP_UPDATE_TIME) == 2:
            raise ValueError(f'Invalid value for EXP_UPDATE_TIME: {self.EXP_UPDATE_TIME}')

        if not len(self.HOST_IP.split('.')) == 4 and all(map(lambda x: isinstance(x, int), self.HOST_IP.split('.'))):
            raise ValueError(f'Invalid value for HOST_IP: {self.HOST_IP}')
        if not isinstance(self.API_PORT, int):
            raise ValueError(f'Invalid value for API_PORT: {self.API_PORT}')
        if not isinstance(self.CLIENT_ID, int):
            raise ValueError(f'Invalid value for CLIENT_ID: {self.CLIENT_ID}')

        if not self.SQL_SERVER or not self.SQL_USER or not self.SQL_PASSWORD:
            raise ValueError(f'Invalid value for SQL_SERVER, SQL_USER, SQL_PASSWORD: {self.SQL_SERVER}, {self.SQL_USER}, {self.SQL_PASSWORD}')

        if not isinstance(self.GRACE_PERIOD, int):
            raise ValueError(f'Invalid value for GRACE_PERIOD: {self.GRACE_PERIOD}')
        if not isinstance(self.UPDATE_CSV_PATH, str):
            raise ValueError(f'Invalid value for UPDATE_CSV_PATH: {self.UPDATE_CSV_PATH}')

        if not isinstance(self.USE_IBC, bool):
            raise ValueError(f'Invalid value for USE_IBC: {self.USE_IBC}')
        if not isinstance(self.STARTGW_IBC_PATH, str):
            raise ValueError(f'Invalid value for START_GW_PATH: {self.STARTGW_IBC_PATH}')

    def create_time_offset_table(self, start_range: int = -30, end_range: int = 365):
        for day_dif in range(start_range, end_range):
            date = datetime.now(timezone.utc) + timedelta(days=day_dif)

            local_time = date.astimezone(pytz.timezone(self.LOCAL_TZ))
            trade_time = date.astimezone(pytz.timezone(self.EXCHANGE_TZ))
            self.utc_diffs[date.year, date.month, date.day] = (trade_time.utcoffset() - local_time.utcoffset()).total_seconds() / 60 / 60


def tprint(text: str = '', *args, debug: bool = False, **kwargs):
    if (debug and DEBUG_MODE) or not debug:
        print(f'{datetime.now().strftime('%H:%M:%S')} : {text}')


def kill_ibgateway(core: Core):
    tprint('Closing IBGateway...')
    while {psutil.Process(x).name(): x for x in psutil.pids()}.get(core.GATEWAY_PROCESS_NAME, None) is not None:
        conn = Telnet()
        conn.connect(core.IBC_TELNET_IP, core.IBC_TELNET_PORT)
        conn.send('STOP\n')
        conn.close()
        sleep(10)

    tprint(f'IBGateway closed.')



def start_ibgateway(core: Core):
    tprint(f'Starting IbGateway...')

    while {psutil.Process(x).name(): x for x in psutil.pids()}.get(core.GATEWAY_PROCESS_NAME, None) is None:
        if os.path.exists(core.STARTGW_IBC_PATH):
            os.system(core.STARTGW_IBC_PATH)
        else:
            raise Exception(f'IBC not found. Provided path: {core.STARTGW_IBC_PATH}')

        sleep(30)

    tprint(f'IBGateway started.')


def read_data_json(core: Core) -> dict[str: str | float] | defaultdict:
    if os.path.exists(os.path.join(os.path.dirname(__file__), core.JSON_SESSION_FILE_NAME)):
        with open(core.JSON_SESSION_FILE_NAME, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
        return loaded_data

    return defaultdict(dict)


def write_data_json(core, data: dict = None):
    if not isinstance(data, dict):
        raise TypeError('Json data must be a dictionary.')
    tprint(f'Write this to json: {data}')
    loaded_data = read_data_json(core)
    for k, v in data.items():
        if isinstance(v, datetime):
            v = v.timestamp()

        if not isinstance(v, str | int | float | bool):
            raise TypeError('Json data values must be primitive types.')

        loaded_data[k] = v

    with open(core.JSON_SESSION_FILE_NAME, 'w', encoding='utf-8') as f:
        json.dump(loaded_data, f, indent=4)


