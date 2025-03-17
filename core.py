from enum import Enum
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

DEBUG_MODE: bool = False

class Core:
    def __init__(self, TWSCon):
        load_dotenv('.env')

        self.host_ip: str = os.getenv('HOST_IP')
        self.api_port: int = int(os.getenv('API_PORT'))
        self.client_id: int = int(os.getenv('CLIENT_ID'))

        # Microsoft SQL Server Credentials
        self.sql_server: str = os.getenv('SQL_SERVER')
        self.sql_user: str = os.getenv('SQL_USER')
        self.sql_password: str = os.getenv('SQL_PASSWORD')
        self.connection_string: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.sql_server};UID={self.sql_user};PWD={self.sql_password}'

        self.stk_last_update: datetime = datetime.fromtimestamp(float(os.getenv('STK_LAST_UPDATE')))
        self.exp_last_update: datetime = datetime.fromtimestamp(float(os.getenv('EXP_LAST_UPDATE')))

        self.reqId_hashmap: dict = {}
        self.reqId_1: int = 1
        self.reqId_2: int = 100_000_000

        self.underlying_list: dict[str, list[str]] = {'STK': []}

        self.contract_pool: dict[str, list[object]] = { 'STK': [],
                                                        'OPT': [],
                                                        'EXP': []}

        self.immediate_pool: list = []
        self.ip_length: int = 10

        self. writable_pool: list = []

        self.candle_length: str = '15 mins'  # candle length in minutes to build history. Legal units: 1 secs, 5 secs, 10 secs, 15 secs, 30 secs, 1 min, 2 mins, 3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins, 1 hour, 2 hours, 3 hours, 4 hours, 8 hours, 1 day, 1W, 1M

        self.timeout_breaker: dict[int, int] = {4: 20, 8: 40, 26: 120, 52: 180, 9999: 300}

        self.insert_query_max_lines: int = 995

        self.expired_opt_days = 2  # within this many days, an option is considered expired (inclusive)

        #Scheduler times list[hour, minute]
        self.stk_update_timer: list[int] = [18, 0]
        self.exp_update_timer: list[int] = [22, 30]
        self.stk_update_timer: datetime = datetime.today().replace(hour=self.stk_update_timer[0], minute=self.stk_update_timer[1], second=0, microsecond=0)
        self.exp_update_timer: datetime = datetime.today().replace(hour=self.exp_update_timer[0], minute=self.exp_update_timer[1], second=0, microsecond=0)

        self.timer_exclude_days: list[int] = [5, 6]
        self.startup = True

        self.randomize_opts = True

        self.exp_opt_file_name = 'expired_option_contracts.pkl'

        #List Updater
        self.grace_period: int = int(os.getenv('GRACE_PERIOD'))
        self.update_csv_path: str = os.getenv('UPDATE_CSV_PATH')
        self.extra_symbols: list[str] = os.getenv('EXTRA_SYMBOLS').split(',')

        self.connection_status = ConnectionStatus.DISCONNECTED

        self.tws_con = self.build_tws_connection(TWSCon)

    def build_tws_connection(self, TWSCon):
        return TWSCon(core=self)

def tprint(text: str = '', *args, debug: bool = False, **kwargs):
    if (debug and DEBUG_MODE) or not debug:
        print(f'{datetime.now().strftime('%H:%M:%S')} : {text}')


class ConnectionStatus(Enum):
    DISCONNECTED = 0
    CONNECTED = 1
    RECONNECTING = 2
