import yfinance as yf
import datetime as datetime
from stocks_db import Stocks_DB
# from concurrent.futures import ThreadPoolExecutor
# from itertools import repeat
# import pandas as pd

# Stock checker function
def check_stock(ticker):
    db = Stocks_DB()  # Create a new DB connection in this thread
    stock = yf.Ticker(ticker)
    df = stock.history(period="1y")

    if len(df) < 200:
        print(f"Not enough data for {ticker}")
        return

    df["SMA_200"] = df["Close"].rolling(window=200).mean()

    current_price = df["Close"].iloc[-1]
    current_sma200 = df["SMA_200"].iloc[-1]
    sma_30_days_ago = df["SMA_200"].iloc[-30]
    week_52_high = df["Close"].max()

    is_above_sma = current_price > current_sma200
    sma_uptrend = current_sma200 > sma_30_days_ago
    near_52_week_high = current_price >= (0.95 * week_52_high)

    # Create a dictionary to store the stock data
    stock_data = {
        "ticker": ticker, 
        "current_price": current_price, 
        "moving_avg_200": current_sma200, 
        "high_52_week": week_52_high, 
        "above_sma": bool(is_above_sma),
        "sma_uptrend": bool(sma_uptrend),
        "near_high": bool(near_52_week_high),
        "date_checked": datetime.datetime.now().date(),
        "last_price_date": df.index[-1].date()
    }

    # print(stock_data)

    # Insert or update data using the global cursor
    db.insert_or_update_data(stock_data)
    db.db_close()

    return stock_data

def main():
    # Run the function for a list of tickers using multithreading
    tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
    # tickers = get_sp500_tickers()
    # with ThreadPoolExecutor() as executor:
    #   executor.map(check_stock, tickers)
    # print(get_russell_2000())

if __name__ == "__main__":
    main()