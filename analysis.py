import yfinance as yf
import datetime as datetime
from stocks_db import Stocks_DB

def setup(ticker):
    db = Stocks_DB()  # Create a new DB connection in this thread
    stock = yf.Ticker(ticker)
    df = stock.history(period="1y")
    return db, df

def moving_avg(df, days : int):
    if not isinstance(days, int):
        return None
    df[f"SMA_{days}"] = df["Close"].rolling(window=days).mean()
    return df

# Stock checker function
def minervini(ticker):
    db, df = setup(ticker)

    if len(df) < 200:
        print(f"Not enough data for {ticker}")
        return

    df = moving_avg(df, 200)

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
    db.insert_or_update_minervini(stock_data)
    db.db_close()

    return stock_data

# Stock checker function
def golden_cross(ticker, long_avg=200, short_avg=50):
    db, df = setup(ticker)

    if len(df) < long_avg:
        print(f"Not enough data for {ticker}")
        return

    df = moving_avg(df, long_avg)
    df = moving_avg(df, short_avg)

    current_sma50 = df["SMA_50"].iloc[-1]
    current_sma200 = df["SMA_200"].iloc[-1]

    sma50_beating_sma200 = current_sma50 > current_sma200

    # Create a dictionary to store the stock data
    golden_cross_data = {
        "ticker": ticker, 
        "moving_avg_50": current_sma50,
        "moving_avg_200": current_sma200, 
        "recent_uptrend": bool(sma50_beating_sma200),
        "date_checked": datetime.datetime.now().date(),
        "last_price_date": df.index[-1].date()
    }

    # print(golden_cross_data)

    # Insert or update data using the global cursor
    db.insert_or_update_golden_cross(golden_cross_data)
    db.db_close()

    return golden_cross_data

def stock_metadata(ticker):
    db = Stocks_DB()
    stock = yf.Ticker(ticker)
    info = stock.info

    metadata = {
        "ticker": ticker,
        "name": info.get('longName', None),
        "sector": info.get('sector', None),
        "industry": info.get('industry', None)    
    }

    db.insert_or_update_metadata(metadata)
    db.db_close()
    
    return metadata

def main():
    # Run the function for a list of tickers using multithreading
    tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
    # tickers = get_sp500_tickers()
    # with ThreadPoolExecutor() as executor:
    #   executor.map(check_stock, tickers)
    # print(get_russell_2000())
    # print(golden_cross(tickers[0]))
    # print(stock_metadata(tickers[0]))

if __name__ == "__main__":
    main()