from ast import literal_eval
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import psutil
import pytz
from typing import Callable

load_dotenv('.env')

DEBUG_MODE: bool = False


class Core:
    def __init__(self):
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
        self.connection_string: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SQL_SERVER};UID={self.SQL_USER};PWD={self.SQL_PASSWORD}'

        # IBC settings for TWS API restart
        self.USE_IBC: bool = bool(os.getenv('USE_IBC'))
        self.STARTGW_IBC_PATH: str = os.getenv('START_GW_PATH')

        # Further user defined settings
        self.IP_LENGTH: int = 10  # length for immediate_pool length. Queue between pipeline_builder and pipeline_handler
        self.TIMER_EXCLUDE_DAYS: list[int] = [5, 6]  # skip trading day triggers on these weekdays (0-6)
        self.INSERT_QUERY_MAX_LINES: int = 995  # max amount of inserts per query
        self.GLITCH_DETECTOR_THRESHOLD: int = 900  # threshold in seconds after which tws_api glitching is assumed
        self.EXPIRED_OPT_DAYS = 2  # threshold in days after that an option is considered expired (inclusive)

        self.LOCAL_TZ: str = 'Europe/Berlin'  # name of local timezone
        self.EXCHANGE_TZ: str = 'America/New_York'  # name of exchange timezone
        self.NORMALIZED_TIME_DIFF: int = 6  # usual time difference between local and exchange

        self.EXP_OPT_FILE_NAME: str = 'expired_option_contracts.pkl'

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
        self.stk_last_update: datetime = datetime.fromtimestamp(float(os.getenv('STK_LAST_UPDATE')))
        self.exp_last_update: datetime = datetime.fromtimestamp(float(os.getenv('EXP_LAST_UPDATE')))

        self.stk_update_timer: datetime = datetime.today().replace(hour=self.STK_UPDATE_TIME[0], minute=self.STK_UPDATE_TIME[1], second=0, microsecond=0)
        self.exp_update_timer: datetime = datetime.today().replace(hour=self.EXP_UPDATE_TIME[0], minute=self.EXP_UPDATE_TIME[1], second=0, microsecond=0)
        self.monday_roll_timer: datetime = next(filter(lambda x: x.weekday() == 0, ((datetime.today() + timedelta(days=x + 1) for x in range(0, 7))))).replace(hour=6, minute=0)

        self.startup: bool = True
        self.tws_con: "TWSCon" = None

        self.last_request: datetime = None
        self.last_receive: datetime = None

        self.utc_diffs: dict[tuple[int]: int] = {}
        self.create_time_offset_table()

    def write_tws_connection(self, TWSCon):
        self.tws_con = TWSCon

    def create_time_offset_table(self):
        for day_dif in range(365):
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
    else:
        raise Exception('IBGateway process not found.')

    tprint(f'IBGateway closed.')


def start_ibgateway(core):
    if os.path.exists(core.STARTGW_IBC_PATH):
        os.system(core.STARTGW_IBC_PATH)
    else:
        tprint(f'IBC not found. Provided path: {core.STARTGW_IBC_PATH}')

    tprint(f'IBGateway started.')



