import yfinance as yf
import pandas as pd
import datetime as datetime
from stocks_db import Stocks_DB
import logging
import time
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from stocks_db import Stocks_DB
from tqdm import tqdm
import matplotlib.pyplot as plt
import tickers

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

def calculate_stocks_above_ma(period="1y", ma_days=20, sample_size=None):
    """
    Calculate percentage of S&P 500 stocks above their moving average over time
    
    Args:
        period: Time period to analyze (e.g., "1y", "6mo", "3mo")
        ma_days: Moving average period in days
        sample_size: Optional number of stocks to sample (for testing)
    
    Returns:
        DataFrame with dates and percentage of stocks above MA
    """
    # Get S&P 500 tickers
    sp500_tickers = tickers.get_sp500()
    
    # For testing with smaller sample
    if sample_size:
        sp500_tickers = sp500_tickers[:sample_size]
    
    # Initialize dictionary to store daily counts
    result_data = {}
    
    print(f"Processing {len(sp500_tickers)} stocks...")
    
    # Process each ticker with a progress bar
    for ticker in tqdm(sp500_tickers):
        try:
            # Get stock data
            stock = yf.Ticker(ticker)
            df = stock.history(period=period).reset_index()
            
            # Skip if not enough data
            if len(df) < ma_days + 5:
                continue
                
            # Calculate moving average
            df = moving_avg(df, ma_days)
            
            # Determine if stock is above or below MA for each date
            df["Above_MA"] = df["Close"] > df[f"SMA_{ma_days}"]
            
            # Iterate through each row after MA is calculated
            for _, row in df[~df[f"SMA_{ma_days}"].isna()].iterrows():
                date = row["Date"].strftime("%Y-%m-%d")
                
                # Initialize counter for this date if not exists
                if date not in result_data:
                    result_data[date] = {"above_ma": 0, "total": 0}
                
                # Update counters
                result_data[date]["total"] += 1
                if row["Above_MA"]:
                    result_data[date]["above_ma"] += 1
                    
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
    
    # Convert results to DataFrame
    dates = []
    percentages = []
    
    for date, data in sorted(result_data.items()):
        if data["total"] > 0:  # Avoid division by zero
            percentage = (data["above_ma"] / data["total"]) * 100
            dates.append(date)
            percentages.append(percentage)
    
    result_df = pd.DataFrame({
        "Date": pd.to_datetime(dates),
        "Percentage_Above_MA": percentages
    })
    
    result_df.set_index("Date", inplace=True)
    result_df.sort_index(inplace=True)
    
    return result_df

def plot_percentage_above_ma(result_df, ma_days=20, period="1y"):
    """Plot the percentage of stocks above their moving average"""
    plt.figure(figsize=(12, 6))
    
    # Plot the percentage line
    plt.plot(result_df.index, result_df["Percentage_Above_MA"], linewidth=2)
    
    # Add horizontal lines at key levels
    plt.axhline(y=90, color='r', linestyle='--', alpha=0.5)
    # plt.axhline(y=20, color='g', linestyle='--', alpha=0.5)
    # plt.axhline(y=50, color='k', linestyle='--', alpha=0.3)
    
    # Add labels and title
    plt.title(f"Percentage of S&P 500 Stocks Above {ma_days}-Day Moving Average ({period})")
    plt.ylabel("Percentage (%)")
    plt.ylim(0, 100)
    plt.grid(True, alpha=0.3)
    
    # Add text explaining reference lines
    plt.text(result_df.index[0], 82, "Overbought (90%)", color='r')
    # plt.text(result_df.index[0], 18, "Oversold (20%)", color='g')
    
    plt.tight_layout()
    return plt

def main():
    # Run the function for a list of tickers using multithreading
    # tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
    # tickers = get_sp500_tickers()
    # with ThreadPoolExecutor() as executor:
    #   executor.map(check_stock, tickers)
    # print(get_russell_2000())
    # print(golden_cross(tickers[0]))
    # print(stock_metadata(tickers[0]))
    # vix()
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

    # result_df = calculate_stocks_above_ma()
    
    # # Print some statistics
    # print("\nResults Summary:")
    # print(f"Period analyzed: 1 year")
    # print(f"Moving Average: 20 days")
    # print(f"Date range: {result_df.index.min()} to {result_df.index.max()}")
    # print(f"Current percentage above MA: {result_df['Percentage_Above_MA'].iloc[-1]:.2f}%")
    # print(f"Average percentage above MA: {result_df['Percentage_Above_MA'].mean():.2f}%")
    # print(f"Max percentage above MA: {result_df['Percentage_Above_MA'].max():.2f}%")
    # print(f"Min percentage above MA: {result_df['Percentage_Above_MA'].min():.2f}%")
    
    # # Plot results
    # plt = plot_percentage_above_ma(result_df)
    # plt.show()
    pass

if __name__ == "__main__":
    main()