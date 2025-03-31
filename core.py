from ast import literal_eval
from enum import Enum
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import pytz

load_dotenv('.env')

DEBUG_MODE: bool = False


class Core:
    def __init__(self):
        #  General Settings:
        self.candle_length: str = os.getenv('CANDLE_LENGTH')
        self.randomize_opts = literal_eval(os.getenv('RANDOMIZE_OPTS'))
        self.stk_update_time: list[int] = literal_eval(os.getenv('STK_UPDATE_TIME'))  # list[hour, minute]
        self.exp_update_time: list[int] = literal_eval(os.getenv('EXP_UPDATE_TIME'))  # list[hour, minute]

        # Constituents list updater
        self.grace_period: int = int(os.getenv('GRACE_PERIOD'))  # grace period after STK left index
        self.update_csv_path: str = os.getenv('UPDATE_CSV_PATH')
        self.extra_symbols: list[str] = os.getenv('EXTRA_SYMBOLS').split(',')

        # TWS API credentials
        self.host_ip: str = os.getenv('HOST_IP')
        self.api_port: int = int(os.getenv('API_PORT'))
        self.client_id: int = int(os.getenv('CLIENT_ID'))

        # Microsoft SQL Server credentials
        self.sql_server: str = os.getenv('SQL_SERVER')
        self.sql_user: str = os.getenv('SQL_USER')
        self.sql_password: str = os.getenv('SQL_PASSWORD')
        self.connection_string: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.sql_server};UID={self.sql_user};PWD={self.sql_password}'

        # Pipeline time triggers
        self.stk_last_update: datetime = datetime.fromtimestamp(float(os.getenv('STK_LAST_UPDATE')))
        self.exp_last_update: datetime = datetime.fromtimestamp(float(os.getenv('EXP_LAST_UPDATE')))

        # Settings
        self.reqId_hashmap: dict = {}
        self.reqId_1: int = 1
        self.reqId_2: int = 100_000_000

        self.underlying_list: dict[str, list[str]] = {'STK': []}

        self.contract_pool: dict[str, list[object]] = {'STK': [],
                                                       'OPT': [],
                                                       'EXP': []}

        self.immediate_pool: list = []
        self.ip_length: int = 10

        self.writable_pool: list = []

        self.timeout_breaker: dict[int, int] = {4: 20, 8: 40, 26: 120, 52: 180, 9999: 300}

        self.insert_query_max_lines: int = 995

        self.expired_opt_days = 2  # within this many days, an option is considered expired (inclusive)

        self.stk_update_timer: datetime = datetime.today().replace(hour=self.stk_update_time[0], minute=self.stk_update_time[1], second=0, microsecond=0)
        self.exp_update_timer: datetime = datetime.today().replace(hour=self.exp_update_time[0], minute=self.exp_update_time[1], second=0, microsecond=0)
        self.timer_exclude_days: list[int] = [5, 6]
        self.monday_roll_timer: datetime = next(filter(lambda x: x.weekday() == 0, ((datetime.today() + timedelta(days=x + 1) for x in range(0, 7))))).replace(hour=6, minute=0)

        self.exp_opt_file_name = 'expired_option_contracts.pkl'

        self.startup = True
        self.tws_con = None

        self.last_request: datetime | None = None
        self.last_receive: datetime | None = None
        self.glitch_detector_threshold: int = 900  #in secs

        self.local_tz: str = 'Europe/Berlin'
        self.trade_tz: str = 'America/New_York'
        self.normalized_time_diff: int = 6
        self.utc_diffs: dict[tuple[int]: int] = {}

    def write_tws_connection(self, TWSCon):
        self.tws_con = TWSCon

    def create_time_offset_table(self):
        for day_dif in range(365):
            date = datetime.now(timezone.utc) - timedelta(days=day_dif)

            local_time = date.astimezone(pytz.timezone(self.local_tz))
            trade_time = date.astimezone(pytz.timezone(self.trade_tz))
            self.utc_diffs[date.year, date.month, date.day] = (trade_time.utcoffset() - local_time.utcoffset()).total_seconds() / 60 / 60

def tprint(text: str = '', *args, debug: bool = False, **kwargs):
    if (debug and DEBUG_MODE) or not debug:
        print(f'{datetime.now().strftime('%H:%M:%S')} : {text}')


class ConnectionStatus(Enum):
    DISCONNECTED = 0
    CONNECTED = 1
    RECONNECTING = 2
