from datetime import datetime, timedelta
from dotenv import load_dotenv
import os


class Core:
    def __init__(self):
        load_dotenv('.env')

        self.host_ip: str = os.getenv('HOST_IP')
        self.api_port: int = int(os.getenv('API_PORT'))
        self.client_id: int = int(os.getenv('CLIENT_ID'))

        # Microsoft SQL Server Credentials
        self.sql_server: str = os.getenv('SQL_SERVER')
        self.sql_user: str = os.getenv('SQL_USER')
        self.sql_password: str = os.getenv('SQL_PASSWORD')
        self.connection_string: str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.sql_server};UID={self.sql_user};PWD={self.sql_password}'

        self.reqId_hashmap: dict = {}
        self.reqId_1: int = 1
        self.reqId_2: int = 100_000_000

        self.underlying_list: dict[str, str] = self.underlyings()

        self.contract_pool: dict[str, list[object]] = {
                                                        'STK': [],
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


    def underlyings(self) -> dict[str, list[str]]:
        underlyings: dict = {'STK': ['USO', 'SPY', 'QQQ', 'IWM', 'GLD', 'TLT', 'IEF', 'LQD',
                                          'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE', 'LNT',
                                          'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN', 'APH',
                                          'ADI',
                                          'ANSS', 'AON', 'APA', 'AAPL', 'AMAT', 'APTV', 'ACGL', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB',
                                          'AVY',
                                          'AXON', 'BKR', 'BALL', 'BAC', 'BK', 'BBWI', 'BAX', 'BDX', 'BRK.B', 'BBY', 'TECH', 'BIIB', 'BLK', 'BX', 'BA', 'BKNG',
                                          'BWA',
                                          'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF.B', 'BLDR', 'BG', 'CDNS', 'CZR', 'CPT', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR',
                                          'CTLT', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'COR', 'CNC', 'CNP', 'CF', 'CHRW', 'CRL', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD',
                                          'CI',
                                          'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'COP', 'ED', 'STZ', 'CEG',
                                          'COO',
                                          'CPRT', 'GLW', 'CPAY', 'CTVA', 'CSGP', 'COST', 'CTRA', 'CCI', 'CSX', 'CMI', 'CVS', 'DHR', 'DRI', 'DVA', 'DAY', 'DECK', 'DE',
                                          'DAL',
                                          'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DHI', 'DTE', 'DUK', 'DD', 'EMN', 'ETN', 'EBAY',
                                          'ECL',
                                          'EIX', 'EW', 'EA', 'ELV', 'LLY', 'EMR', 'ENPH', 'ETR', 'EOG', 'EPAM', 'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EG',
                                          'EVRG',
                                          'ES', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FDS', 'FICO', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FI', 'FMC',
                                          'F',
                                          'FTNT', 'FTV', 'FOXA', 'FOX', 'BEN', 'FCX', 'GRMN', 'IT', 'GE', 'GEHC', 'GEV', 'GEN', 'GNRC', 'GD', 'GIS', 'GM', 'GPC', 'GILD',
                                          'GPN',
                                          'GL', 'GS', 'HAL', 'HIG', 'HAS', 'HCA', 'DOC', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM',
                                          'HPQ',
                                          'HUBB', 'HUM', 'HBAN', 'HII', 'IBM', 'IEX', 'IDXX', 'ITW', 'ILMN', 'INCY', 'IR', 'PODD', 'INTC', 'ICE', 'IFF', 'IP', 'IPG',
                                          'INTU',
                                          'ISRG', 'IVZ', 'INVH', 'IQV', 'IRM', 'JBHT', 'JBL', 'JKHY', 'J', 'JNJ', 'JCI', 'JPM', 'JNPR', 'K', 'KVUE', 'KDP', 'KEY', 'KEYS',
                                          'KMB',
                                          'KIM', 'KMI', 'KLAC', 'KHC', 'KR', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LDOS', 'LEN', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW',
                                          'LULU',
                                          'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MTCH', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK', 'META', 'MET',
                                          'MTD',
                                          'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MRNA', 'MHK', 'MOH', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI',
                                          'NDAQ',
                                          'NTAP', 'NFLX', 'NEM', 'NWSA', 'NWS', 'NEE', 'NKE', 'NI', 'NDSN', 'NSC', 'NTRS', 'NOC', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR',
                                          'NXPI',
                                          'ORLY', 'OXY', 'ODFL', 'OMC', 'ON', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PANW', 'PARA', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR',
                                          'PEP',
                                          'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PTC', 'PSA',
                                          'PHM',
                                          'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RVTY', 'RHI', 'ROK', 'ROL', 'ROP',
                                          'ROST',
                                          'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SJM', 'SNA', 'SOLV', 'SO', 'LUV', 'SWK', 'SBUX',
                                          'STT', 'STLD', 'STE', 'SYK', 'SMCI', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TRGP', 'TGT', 'TEL', 'TDY', 'TFX',
                                          'TER',
                                          'TSLA', 'TXN', 'TXT', 'TMO', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TRMB', 'TFC', 'TYL', 'TSN', 'USB', 'UBER', 'UDR', 'ULTA', 'UNP',
                                          'UAL', 'UPS', 'URI', 'UNH', 'UHS', 'VLO', 'VTR', 'VLTO', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VTRS', 'VICI', 'V', 'VMC', 'WRB', 'WAB',
                                          'WBA',
                                          'WMT', 'DIS', 'WBD', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WRK', 'WY', 'WMB', 'WTW', 'GWW', 'WYNN', 'XEL', 'XYL',
                                          'YUM',
                                          'ZBRA', 'ZBH', 'ZTS',
                                          'PLTR', 'DELL', 'ERIE',
                                          'AAL', 'ETSY', 'BIO']}
        return underlyings

    def set_TWSCon(self, TWSCon):
        self.TWSCon = TWSCon
        print(123)

def tprint(text: str = '', *args, **kwargs) -> str:
    print(f'{datetime.now().strftime('%H:%M:%S')} : {text}')
