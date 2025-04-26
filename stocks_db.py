import sqlite3
import pandas as pd

class Stocks_DB:
    def __init__(self):
        self.conn = sqlite3.connect("stocks.db", isolation_level=None)  # Autocommit mode
        self.cursor = self.conn.cursor()

    # Close the connection when all operations are done
    def db_close(self):
        self.conn.close()

    # Function to create the tables if it they don't exist (run once)
    def setup_database(self):
        self.cursor.executescript('''
        CREATE TABLE IF NOT EXISTS minervini (
            ticker TEXT PRIMARY KEY,
            current_price REAL,
            moving_avg_200 REAL,
            high_52_week REAL,
            above_sma BOOLEAN,
            sma_uptrend BOOLEAN,
            near_high BOOLEAN,
            date_checked DATE,
            last_price_date DATE
        );
                                  
        CREATE TABLE IF NOT EXISTS golden_cross (
            ticker TEXT PRIMARY KEY,
            moving_avg_200 REAL,
            moving_avg_50 REAL,
            recent_uptrend BOOLEAN,
            date_checked DATE,
            last_price_date DATE
        );
                                  
        CREATE TABLE IF NOT EXISTS stock_metadata (
            ticker TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            industry TEXT
        );
        ''') 

    # Function to insert or update data (using INSERT OR REPLACE)
    def insert_or_update_minervini(self, stock_data):
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO minervini (ticker, current_price, moving_avg_200, high_52_week, 
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

    def insert_or_update_metadata(self, meta_data):
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO stock_metadata (ticker, company_name, sector, industry)
            VALUES (?, ?, ?, ?)
            ''', (
                meta_data['ticker'],
                meta_data['name'],
                meta_data['sector'],
                meta_data['industry'],
            ))
        except Exception as e:
            print(f"Error inserting data for {meta_data['ticker']}: {e}")

    def insert_or_update_golden_cross(self, cross_data):
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO golden_cross (ticker, moving_avg_200, moving_avg_50, recent_uptrend, date_checked, last_price_date)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                cross_data['ticker'],
                cross_data['moving_avg_200'],
                cross_data['moving_avg_50'],
                cross_data['recent_uptrend'],
                cross_data['date_checked'],
                cross_data['last_price_date']
            ))
        except Exception as e:
            print(f"Error inserting data for {cross_data['ticker']}: {e}")

    def get_table_names(self):
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        return {row[0] for row in cursor.fetchall()}

    # Function to get the db as a DataFrame
    def to_pd_df(self, table="stock_metadata"):
        allowed_tables = self.get_table_names()
        if table not in allowed_tables:
            raise ValueError(f"Invalid table name: {table}")
        return pd.read_sql(f"SELECT * FROM {table}", self.conn)