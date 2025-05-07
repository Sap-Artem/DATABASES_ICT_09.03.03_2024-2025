import json
import os
import sys

import requests
import logging
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

# Load config
load_dotenv("env.env")

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("xrp_import")

# Constants
BATCH_SIZE = 50
API_URL = "https://api.xrpscan.com/api/v1/account"
BLOCKCHAIN_NAME = "ripple"

# DB connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Step 1: Get blockchain_id
def get_blockchain_id(name="ripple"):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM blockchains WHERE name = %s", (name,))
            result = cur.fetchone()
            return result[0] if result else None

# Step 2: Get tracked wallets
def get_wallets(blockchain_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM wallets WHERE blockchain_id = %s", (blockchain_id,))
            return [row[0] for row in cur.fetchall()]

# Step 3: Determine direction
def determine_direction(from_addr, to_addr, tracked):
    from_in = from_addr in tracked
    to_in = to_addr in tracked
    if from_in and to_in:
        return 'internal'
    elif to_in:
        return 'incoming'
    elif from_in:
        return 'outgoing'
    return None

# Step 4: Fetch transactions from XRPScan
def fetch_transactions_for_wallet(address, start, end):
    url = f"{API_URL}/{address}/transactions"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Разбираем ответ от API
        data = response.json()
        transactions = data.get("transactions", [])

        if not isinstance(transactions, list):
            logger.error("Expected 'transactions' to be a list.")
            return []

        # Убедимся, что start и end — строки
        if isinstance(start, datetime):
            start_date = start
        else:
            start_date = datetime.strptime(start, "%Y-%m-%d")

        if isinstance(end, datetime):
            end_date = end
        else:
            end_date = datetime.strptime(end, "%Y-%m-%d")

        logger.info(f"Filtering transactions between {start_date} and {end_date}")

        filtered = []
        for tx in transactions:
            try:
                # Получаем дату транзакции и парсим её
                tx_date_raw = tx.get("date")
                if isinstance(tx_date_raw, datetime):
                    tx_date = tx_date_raw
                elif isinstance(tx_date_raw, str):
                    # Обрабатываем строку даты в нужном формате
                    try:
                        tx_date = datetime.strptime(tx_date_raw, "%Y-%m-%dT%H:%M:%S.%fZ")
                        if start_date <= tx_date <= end_date:
                            filtered.append(tx)
                        else:
                            logger.info(f"Transaction {tx.get('hash')} is outside the date range.")
                    except ValueError:
                        # Если формат даты отличается, попробуем другой
                        logger.warning(f"Invalid date format for transaction: {tx_date_raw}")
                        continue
                else:
                    logger.warning(f"Unknown date format for transaction: {tx_date_raw}")
                    continue

                # Проверяем, попадает ли транзакция в указанный диапазон
                if start_date <= tx_date <= end_date:
                    filtered.append(tx)
                else:
                    logger.info(f"Transaction {tx.get('hash')} is outside the date range.")

            except Exception as e:
                logger.warning(f"Failed to parse transaction date for {tx.get('hash')}: {e}")

        logger.info(f"Fetched {len(filtered)} valid transactions for address {address} between {start_date} and {end_date}")
        return filtered

    except Exception as e:
        logger.error(f"Failed to fetch transactions for {address}: {e}")
        return []

# Step 5: Parse and filter transactions
def process_transactions(data, tracked_addresses, blockchain_id):
    parsed = []
    for tx in data:
        try:
            tx_hash = tx.get("hash")
            timestamp = datetime.fromisoformat(tx.get("date").replace("Z", "+00:00"))

            from_address = tx.get("Account", "").strip()
            to_address = tx.get("Destination", "").strip()

            # Получаем сумму
            delivered = tx.get("meta", {}).get("delivered_amount")
            if isinstance(delivered, dict):
                amount = float(delivered.get("value", 0)) / 1_000_000
            else:
                amount_raw = tx.get("Amount", {})
                amount_value = float(amount_raw.get("value", 0)) if isinstance(amount_raw, dict) else float(amount_raw)
                amount = amount_value / 1_000_000

            # Комиссия
            fee_drops = tx.get("Fee", "0")
            gas_fee = float(fee_drops) / 1_000_000

            direction = determine_direction(from_address, to_address, tracked_addresses)
            if not direction:
                logger.info(f"Skipping tx {tx_hash} — direction is None (from: {from_address}, to: {to_address})")
                continue

            parsed.append((
                blockchain_id, tx_hash, timestamp, from_address, to_address, direction,
                "XRP", "", amount, gas_fee, "transfer", False, 0, 0
            ))
        except Exception as e:
            logger.error(f"Error parsing tx: {e}")
    return parsed


# Step 6: Save to DB
def insert_transactions(transactions):
    insert_query = """
        INSERT INTO transactions (
            blockchain_id, tx_hash, timestamp, from_address, to_address, direction,
            token_symbol, contract_address, amount, gas_fee, tx_type, is_suspicious,
            usdt_volume, btc_volume
        )
        VALUES %s
        ON CONFLICT (blockchain_id, tx_hash) DO NOTHING
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(transactions), BATCH_SIZE):
                batch = transactions[i:i + BATCH_SIZE]
                try:
                    execute_values(cur, insert_query, batch)
                    conn.commit()
                    logger.info(f"Inserted batch of {len(batch)} transactions.")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"DB insert error: {e}")

# Main pipeline
def import_xrp_transactions(date_start, date_end):
    blockchain_id = get_blockchain_id(BLOCKCHAIN_NAME)
    if not blockchain_id:
        logger.error(f"Blockchain '{BLOCKCHAIN_NAME}' not found.")
        return

    wallets = get_wallets(blockchain_id)
    all_transactions = []

    for wallet in wallets:
        tx_data = fetch_transactions_for_wallet(wallet, date_start, date_end)
        processed = process_transactions(tx_data, wallets, blockchain_id)
        all_transactions.extend(processed)

    if all_transactions:
        insert_transactions(all_transactions)
    else:
        logger.info("No valid transactions found.")

# Example
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fetch_xrp_transactions.py <start_date> <end_date>")
        print("Date format: YYYY-MM-DD")
        sys.exit(1)

    start = datetime.fromisoformat(sys.argv[1])
    end = datetime.fromisoformat(sys.argv[2])
    import_xrp_transactions(start, end)
