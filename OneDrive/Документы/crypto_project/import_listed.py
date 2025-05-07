import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import logging

# Загружаем конфигурацию    
load_dotenv(dotenv_path="env.env")

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("import_prices")

# Подключение к БД
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Вставка цен в token_prices
def insert_token_prices(prices: list):
    """
    prices: список словарей вида:
    {
        "contract_address": str,
        "token_symbol": str,
        "timestamp": datetime,
        "price_usdt": float,
        "price_btc": float
    }
    """
    if not prices:
        return

    query = """
        INSERT INTO token_prices (
            contract_address, token_symbol, timestamp, price_usdt, price_btc
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (contract_address, timestamp) DO NOTHING
    """

    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        data_to_insert = [
            (
                entry["contract_address"],
                entry["token_symbol"],
                entry["timestamp"],
                entry.get("price_usdt", 0),
                entry.get("price_btc", 0)
            )
            for entry in prices
        ]
        cursor.executemany(query, data_to_insert)
        connection.commit()
        logger.info(f"Inserted {len(data_to_insert)} token price records.")
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Failed to insert token prices: {e}")
    finally:
        if connection:
            connection.close()
