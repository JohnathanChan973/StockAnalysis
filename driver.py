import logging
import time
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from stocks_db import Stocks_DB
import analysis
import tickers
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    filename="stocks_analysis.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

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
def run_batches(tickers, func=analysis.stock_metadata, batch_size=20, max_workers=10, max_retries=2):
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

# Testbed main
def main():
    #db = Stocks_DB()
    #db.setup_database()

    # analyze_tickers = tickers.get_russell2000()
    analyze_tickers = ["AAPL", "TSLA"]
    run_batches(analyze_tickers, func=analysis.golden_cross)

    # Optional: save snapshot
    # df = db.to_pd_df()
    # df.to_csv("stocks_db_snapshot.csv", index=False)
    #print(db.get_table_names())
    #db.db_close()

if __name__ == "__main__":
    main()
