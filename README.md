![image](https://github.com/user-attachments/assets/f60f16e1-eb24-4852-bde7-f1a48d3d1256)

# TWS API Option Price Scraper</ins>

This is a TWS API option price scraper that generates a custom option price history SQL database for all provided underlying symbols across all strikes, expiries, and rights. The script utilizes the IBKR API, either via Trader Workstation or IB Gateway, and requires a local Microsoft SQL Server. 

This project aims to provide a comprehensive and customizable solution for collecting and storing option price data.

For use please configure .env_rename and rename it to .env thereafter. For more detailed usage settings please take a look at the initializer of core.py and customize fields declared as such.

This scripts supports Live and Paper Trading accounts from IBKR or official resellers.

## <ins>Features:</ins>
> * Adaptive prioritization of requests by moneyness and expiry.
> 
> * Custom security symbol source list.
> 
> * Automatic queue management.
> 
> * Smart SQL table management by date and underlying. (e.g. AAPL_OPT_28Mar25)
> 
> * Automated reconnecting.
> 
> * Caching of queue status.
> 
> * Customizable logging.
> 
> * Multi-threaded execution for improved performance due to API response times.
> 
> * Automated IBC restart (optional).

## <ins>Preconfiguration:</ins>
> * Queue size adjusted for average TWS API speed.
> * Contains and maintains S&P500 constituents list as well as some evergreens like SPY, TLT, USO, QQQ, etc.
> * TWS API Port: 7498


## <ins>Requirements:</ins>
> * Python 3.12+
> 
> * UV package manager: https://docs.astral.sh/uv/
> 
> * IBC (optional): https://github.com/IbcAlpha/IBC
> 
> * (local) Microsoft SQL Server (TSQL). Tested for SQL Server 2022 v16
> 
> * IBKR API: Trader Workstation or IB Gateway. API v10.30f or newer
> * https://www.interactivebrokers.com/en/trading/ib-gateway-download.php

## <ins>Install:</ins>
> uv sync

## <ins>Run:</ins>
>uv run main.py




