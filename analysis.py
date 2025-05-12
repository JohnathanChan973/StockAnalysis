import yfinance as yf
import datetime as datetime
from stocks_db import Stocks_DB
import logging
import time
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from stocks_db import Stocks_DB
from tqdm import tqdm
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(
    filename="stocks_analysis.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

def setup(ticker, period="1y"):
    db = Stocks_DB()  # Create a new DB connection in this thread
    stock = yf.Ticker(ticker)
    df = stock.history(period=period).reset_index()
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
        "last_price_date": df['Date'].iloc[-1].date()
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
        "last_price_date": df['Date'].iloc[-1].date()
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

# Wrapper that logs any exceptions and returns (ticker, success)
def check_stock(ticker, func):
    try:
        result = func(ticker)
        if result:
            logger.info(f"✅ Success: {ticker}")
        return (ticker, True)
    except Exception as e:
        logger.error(f"❌ Failed: {ticker} — {e}")
        return (ticker, False)

# Splits an iterable into chunks
def batch_iterator(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield [first] + list(islice(iterator, size - 1))

# Main batch execution with retries
def run_batches(tickers, func=stock_metadata, batch_size=20, max_workers=10, max_retries=2):
    failed_tickers = []

    for batch_num, batch in enumerate(tqdm(batch_iterator(tickers, batch_size), desc="Batch Progress")):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(tqdm(
                executor.map(lambda ticker: check_stock(ticker, func), batch),
                total=len(batch),
                desc=f"Tickers in Batch {batch_num + 1}"
            ))
        failed_tickers.extend([ticker for ticker, success in results if not success])

    # Retry logic
    for retry in range(1, max_retries + 1):
        if not failed_tickers:
            break

        logger.info(f"🔁 Retry attempt {retry} for {len(failed_tickers)} failed tickers.")
        time.sleep(1)

        retry_failed = []
        for batch in batch_iterator(failed_tickers, batch_size):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(tqdm(
                    executor.map(lambda ticker: check_stock(ticker, func), batch),
                    total=len(batch),
                    desc=f"Retry {retry}"
                ))
            retry_failed.extend([ticker for ticker, success in results if not success])

        failed_tickers = retry_failed

    if failed_tickers:
        logger.warning(f"⚠️ Final failed tickers: {failed_tickers}")
    else:
        logger.info("✅ All tickers processed successfully after retries.")

def vix():
    tickers = ["^VIX", "^VIX3M", "^GSPC"]
    rename_map = {"^VIX": "VIX_Close", "^VIX3M": "VIX3M_Close", "^GSPC": "SP500_close"}

    # Fetch data for all tickers in one call
    df = yf.download(tickers, period="1y", auto_adjust=False)['Close']

    logger.info(f"✅ Success: Got important S&P Info")
    
    # Rename columns for clarity
    df = df.rename(columns=rename_map)

    # Drop rows with missing values (e.g., from different trading calendars)
    df = df.dropna()

    # Reset index and convert Date column
    df = df.reset_index()
    df['Date'] = df['Date'].dt.date

    # Compute the difference between VIX and VIX3M
    df["difference"] = df["VIX_Close"] - df["VIX3M_Close"]

    # Create subplots (2 rows, 1 column)
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8))

    # Plot the S&P 500
    axes[0].plot(df["Date"], df["SP500_close"], color='red')
    axes[0].set_xlabel('Date')
    axes[0].set_ylabel('S&P 500')
    axes[0].set_title('S&P 500 over time')

    # Plot the VIX difference
    axes[1].bar(df["Date"], df["difference"], color='blue')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('VIX 1M - VIX 3M')
    axes[1].set_title('VIX 1M - VIX 3M over time')

    plt.tight_layout()
    plt.show()

def main():
    # Run the function for a list of tickers using multithreading
    # tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
    # tickers = get_sp500_tickers()
    # with ThreadPoolExecutor() as executor:
    #   executor.map(check_stock, tickers)
    # print(get_russell_2000())
    # print(golden_cross(tickers[0]))
    # print(stock_metadata(tickers[0]))
    vix()
    #db = Stocks_DB()
    #db.setup_database()

    # analyze_tickers = tickers.get_russell2000()
    #analyze_tickers = ["AAPL", "TSLA"]
    #run_batches(analyze_tickers, func=golden_cross)

    # Optional: save snapshot
    # df = db.to_pd_df()
    # df.to_csv("stocks_db_snapshot.csv", index=False)
    #print(db.get_table_names())
    #db.db_close()

if __name__ == "__main__":
    main()