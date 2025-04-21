import logging
from itertools import islice, repeat
from concurrent.futures import ThreadPoolExecutor
from stocks_db import Stocks_DB
from analysis import check_stock
import tickers
from tqdm import tqdm
import time

# Set up logging
logging.basicConfig(
    filename="stocks_analysis.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Wrapper that logs any exceptions
def check_stock_safe(ticker):
    try:
        result = check_stock(ticker)
        if result:
            logger.info(f"✅ Success: {ticker}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed: {ticker} — {e}")
        return False

def batch_iterator(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield [first] + list(islice(iterator, size - 1))

def run_batches(tickers, batch_size=20, max_workers=10, max_retries=2):
    failed_tickers = []

    for batch_num, batch in enumerate(tqdm(batch_iterator(tickers, batch_size), desc="Batch Progress")):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(tqdm(executor.map(check_stock_safe, batch), total=len(batch), desc=f"Tickers in Batch {batch_num+1}"))
        failed_tickers.extend([ticker for ticker, success in zip(batch, results) if not success])

    # Retry logic
    for retry in range(1, max_retries + 1):
        if not failed_tickers:
            break

        logger.info(f"🔁 Retry attempt {retry} for {len(failed_tickers)} failed tickers.")
        time.sleep(1)  # optional pause between retries

        retry_failed = []
        for batch in batch_iterator(failed_tickers, batch_size):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(tqdm(executor.map(check_stock_safe, batch), total=len(batch), desc=f"Retry {retry}"))
            retry_failed.extend([ticker for ticker, success in zip(batch, results) if not success])
        
        failed_tickers = retry_failed

    if failed_tickers:
        logger.warning(f"⚠️ Final failed tickers: {failed_tickers}")
    else:
        logger.info("✅ All tickers processed successfully after retries.")

def main():
    db = Stocks_DB()
    db.setup_database()

    # Replace with your real list of tickers
    # tickers = get_sp500_tickers()
    analyze_tickers = tickers.get_russell2000()

    run_batches(analyze_tickers)

    # df = db.to_pd_df()
    # df.to_csv("stocks_db_snapshot.csv", index=False)
    db.db_close()

if __name__ == "__main__":
    main()