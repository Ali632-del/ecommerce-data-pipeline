import pandas as pd
import sqlite3
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_config, get_logger

logger = get_logger('loader')
cfg    = get_config()

DB_PATH = Path(cfg['paths']['raw_data_lake']).parent / 'ecommerce_dw.db'

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def create_tables(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS dim_category (
            category_slug TEXT PRIMARY KEY,
            category_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dim_seller (
            seller_name TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY,
            title TEXT,
            currency TEXT,
            has_variations INTEGER
        );
        CREATE TABLE IF NOT EXISTS fact_product_listing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            category_slug TEXT,
            seller_name TEXT,
            initial_price REAL,
            final_price REAL,
            discount REAL,
            discount_amount REAL,
            is_discounted INTEGER,
            price_bucket TEXT,
            rating REAL,
            ratings_count INTEGER,
            ratings_tier TEXT,
            rating_x_count REAL,
            title_word_count INTEGER,
            has_seller INTEGER
        );
    ''')
    conn.commit()
    logger.info('Tables created OK')

def load_dim_category(df, conn):
    cats = df[['category_slug','category']].drop_duplicates(subset=['category_slug'])
    cats.columns = ['category_slug','category_name']
    cats.to_sql('dim_category', conn, if_exists='replace', index=False)
    logger.info('dim_category: %d rows', len(cats))

def load_dim_seller(df, conn):
    sellers = df[['seller_name']].drop_duplicates()
    sellers.to_sql('dim_seller', conn, if_exists='replace', index=False)
    logger.info('dim_seller: %d rows', len(sellers))

def load_dim_product(df, conn):
    prods = df[['product_id','title','currency','has_variations']].drop_duplicates(subset=['product_id'])
    prods.to_sql('dim_product', conn, if_exists='replace', index=False)
    logger.info('dim_product: %d rows', len(prods))

def load_fact(df, conn):
    cols = ['product_id','category_slug','seller_name','initial_price','final_price',
            'discount','discount_amount','is_discounted','price_bucket','rating',
            'ratings_count','ratings_tier','rating_x_count','title_word_count','has_seller']
    df[cols].to_sql('fact_product_listing', conn, if_exists='replace', index=False)
    logger.info('fact_product_listing: %d rows', len(df))

def run_load():
    proc_file = Path(cfg['paths']['processed_data']) / 'products_cleaned.parquet'
    if not proc_file.exists():
        raise FileNotFoundError(f'Not found: {proc_file}')
    df = pd.read_parquet(proc_file)
    logger.info('Loaded %d rows', len(df))
    conn = get_conn()
    create_tables(conn)
    load_dim_category(df, conn)
    load_dim_seller(df, conn)
    load_dim_product(df, conn)
    load_fact(df, conn)
    conn.close()
    logger.info('Load complete')
    print('Done! Database saved at:', DB_PATH)

if __name__ == '__main__':
    run_load()
