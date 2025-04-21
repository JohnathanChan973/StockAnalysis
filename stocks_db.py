import sqlite3
import pandas as pd

class Stocks_DB:
    def __init__(self):
        self.conn = sqlite3.connect("stocks.db", isolation_level=None)  # Autocommit mode
        self.cursor = self.conn.cursor()

    # Close the connection when all operations are done
    def db_close(self):
        self.conn.close()

    # Function to create the table if it doesn't exist (run once)
    def setup_database(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            ticker TEXT PRIMARY KEY,  -- Make ticker the primary key
            current_price REAL,
            moving_avg_200 REAL,
            high_52_week REAL,
            above_sma BOOLEAN,
            sma_uptrend BOOLEAN,
            near_high BOOLEAN,
            date_checked DATE,
            last_price_date DATE
        )
        ''')

    # Function to insert or update data (using INSERT OR REPLACE)
    def insert_or_update_data(self, stock_data):
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO stock_data (ticker, current_price, moving_avg_200, high_52_week, 
                                            above_sma, sma_uptrend, near_high, date_checked, last_price_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_data['ticker'],
                stock_data['current_price'],
                stock_data['moving_avg_200'],
                stock_data['high_52_week'],
                stock_data['above_sma'],
                stock_data['sma_uptrend'],
                stock_data['near_high'],
                stock_data['date_checked'],
                stock_data['last_price_date']
            ))
        except Exception as e:
            print(f"Error inserting data for {stock_data['ticker']}: {e}")

    # Function to get the db as a DataFrame
    def to_pd_df(self):
        return pd.read_sql("SELECT * FROM stock_data", self.conn)