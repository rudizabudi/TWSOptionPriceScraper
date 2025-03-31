![image](https://github.com/user-attachments/assets/f60f16e1-eb24-4852-bde7-f1a48d3d1256)

# TWS API Option Price Scraper

This is a TWS API option price scraper that generates a custom option price history SQL database for all provided underlying symbols across all strikes, expiries, and rights. The script utilizes the IBKR API, either via Trader Workstation or IB Gateway, and requires a local Microsoft SQL Server. 

This project aims to provide a comprehensive and customizable solution for collecting and storing option price data.

For use please configure .env_rename and rename it to .env thereafter. For more detailed usage settings please take a look at the initializer of core.py and customize fields declared as such.


Features:
> Adaptive prioritization of requests by moneyness and expiry.
> 
> Custom security symbol source list.
> 
> Automatic queue management.
> 
> Smart SQL table management by date and underlying. (e.g. AAPL_OPT_28Mar25)
> 
> Automated reconnection.
> 
> Caching of queue status.
> 
> Customizable logging.
> 
> Multi-threaded execution for improved performance due to API response times.
>

Preconfiguration:
> Queue size adjusted for usual daily TWS API speed.
> Contains and maintains S&P500 constituents list as well as some evergreens like SPY, TLT, USO, QQQ, etc.


Requirements:
> Python 3.12+
> 
> UV package manager: https://docs.astral.sh/uv/
> 
> (local) Microsoft SQL Server (TSQL). Tested for SQL Server 2022 v16
> 
> IBKR API:  Trader Workstation or IB Gateway.
>

Install:
> uv sync

Run:
>uv run main.py




