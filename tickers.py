import pandas as pd
import datetime

def get_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table = pd.read_html(url)[0]  # First table on the page
    return table['Symbol'].tolist()

def get_russell2000():
    # URL to iShares IWM holdings (as of the current date)
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

    # Load CSV into pandas
    df = pd.read_csv(url, skiprows=9)  # skiprows might change if iShares updates format

    df = df.iloc[:-2]
    
    # df[['Ticker', 'Name']].to_csv("iwm_constituents.csv", index=False)

    # Show tickers
    tickers = df['Ticker'].dropna().tolist()
    return tickers

def tickers_to_csv(func):
    tickers = func()  # Get the list of tickers from the function
    df = pd.DataFrame(tickers, columns=["Ticker"])  # Convert the list to a DataFrame
    current_date = datetime.datetime.now().strftime(r"%Y-%m-%d")
    df.to_csv(f"{func.__name__[4:]}_{current_date}.csv", index=False)  # Save DataFrame to CSV