"""
PhonePe Pulse ETL Pipeline
Clones the PhonePe Pulse GitHub repo and loads data into MySQL.
"""

import os
import json
import subprocess
import mysql.connector
from mysql.connector import Error

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
   "password": "your_password",   # <-- update
   
}
REPO_URL = "https://github.com/PhonePe/pulse.git"
REPO_DIR = "pulse"
# ──────────────────────────────────────────────────────────────────────────────


def clone_repo():
    """Clone or pull the PhonePe Pulse repository."""
    if os.path.exists(REPO_DIR):
        print("Repo exists – pulling latest changes...")
        subprocess.run(["git", "-C", REPO_DIR, "pull"], check=True)
    else:
        print("Cloning PhonePe Pulse repo...")
        subprocess.run(["git", "clone", "--depth=1", REPO_URL], check=True)
    print("✅ Repo ready.")


def get_connection():
    """Return a MySQL connection."""
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn


def create_database(conn):
    """Create the database and all 9 tables."""
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")
    cursor.execute("USE phonepe_pulse")

    ddl_statements = [
        # ── Aggregated tables ──────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS aggregated_transaction (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            transaction_type VARCHAR(100),
            transaction_count BIGINT,
            transaction_amount DOUBLE
        )""",
        """CREATE TABLE IF NOT EXISTS aggregated_user (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            registered_users BIGINT,
            app_opens        BIGINT
        )""",
        """CREATE TABLE IF NOT EXISTS aggregated_insurance (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            insurance_type  VARCHAR(100),
            policy_count    BIGINT,
            policy_amount   DOUBLE
        )""",
        # ── Map tables ─────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS map_transaction (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            district        VARCHAR(100),
            transaction_count BIGINT,
            transaction_amount DOUBLE
        )""",
        """CREATE TABLE IF NOT EXISTS map_user (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            district        VARCHAR(100),
            registered_users BIGINT,
            app_opens        BIGINT
        )""",
        """CREATE TABLE IF NOT EXISTS map_insurance (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            district        VARCHAR(100),
            policy_count    BIGINT,
            policy_amount   DOUBLE
        )""",
        # ── Top tables ─────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS top_transaction (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            entity_name     VARCHAR(200),
            entity_type     VARCHAR(50),   -- district / pincode
            transaction_count BIGINT,
            transaction_amount DOUBLE
        )""",
        """CREATE TABLE IF NOT EXISTS top_user (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            entity_name     VARCHAR(200),
            entity_type     VARCHAR(50),
            registered_users BIGINT
        )""",
        """CREATE TABLE IF NOT EXISTS top_insurance (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            state           VARCHAR(100),
            year            INT,
            quarter         INT,
            entity_name     VARCHAR(200),
            entity_type     VARCHAR(50),
            policy_count    BIGINT,
            policy_amount   DOUBLE
        )""",
    ]

    for stmt in ddl_statements:
        cursor.execute(stmt)

    conn.commit()
    cursor.close()
    print("✅ Database & tables created.")


# ── Loaders ────────────────────────────────────────────────────────────────────

def load_aggregated_transaction(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE aggregated_transaction")
    base = os.path.join(REPO_DIR, "data", "aggregated", "transaction", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                for txn in data["data"]["transactionData"]:
                    rows.append((state, int(year), q,
                                 txn["name"],
                                 txn["paymentInstruments"][0]["count"],
                                 txn["paymentInstruments"][0]["amount"]))
    sql = """INSERT INTO aggregated_transaction
             (state, year, quarter, transaction_type, transaction_count, transaction_amount)
             VALUES (%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ aggregated_transaction: {len(rows)} rows loaded.")


def load_aggregated_user(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE aggregated_user")
    base = os.path.join(REPO_DIR, "data", "aggregated", "user", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                summary = data["data"]["aggregated"]
                rows.append((state, int(year), q,
                             summary.get("registeredUsers", 0),
                             summary.get("appOpens", 0)))
    sql = "INSERT INTO aggregated_user (state,year,quarter,registered_users,app_opens) VALUES (%s,%s,%s,%s,%s)"
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ aggregated_user: {len(rows)} rows loaded.")


def load_map_transaction(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE map_transaction")
    base = os.path.join(REPO_DIR, "data", "map", "transaction", "hover", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                for district in data["data"]["hoverDataList"]:
                    rows.append((state, int(year), q,
                                 district["name"],
                                 district["metric"][0]["count"],
                                 district["metric"][0]["amount"]))
    sql = """INSERT INTO map_transaction
             (state,year,quarter,district,transaction_count,transaction_amount)
             VALUES (%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ map_transaction: {len(rows)} rows loaded.")


def load_map_user(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE map_user")
    base = os.path.join(REPO_DIR, "data", "map", "user", "hover", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                for district in data["data"]["hoverData"].items():
                    name, vals = district
                    rows.append((state, int(year), q, name,
                                 vals.get("registeredUsers", 0),
                                 vals.get("appOpens", 0)))
    sql = "INSERT INTO map_user (state,year,quarter,district,registered_users,app_opens) VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ map_user: {len(rows)} rows loaded.")


def load_top_transaction(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE top_transaction")
    base = os.path.join(REPO_DIR, "data", "top", "transaction", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                for entity_type, key in [("district", "districts"), ("pincode", "pincodes")]:
                    for item in data["data"].get(key, []):
                        rows.append((state, int(year), q,
                                     str(item["entityName"]), entity_type,
                                     item["metric"]["count"],
                                     item["metric"]["amount"]))
    sql = """INSERT INTO top_transaction
             (state,year,quarter,entity_name,entity_type,transaction_count,transaction_amount)
             VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ top_transaction: {len(rows)} rows loaded.")


def load_top_user(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE top_user")
    base = os.path.join(REPO_DIR, "data", "top", "user", "country", "india", "state")
    rows = []
    for state in os.listdir(base):
        state_path = os.path.join(base, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for quarter_file in os.listdir(year_path):
                q = int(quarter_file.replace(".json", ""))
                with open(os.path.join(year_path, quarter_file)) as f:
                    data = json.load(f)
                for entity_type, key in [("district", "districts"), ("pincode", "pincodes")]:
                    for item in data["data"].get(key, []):
                        rows.append((state, int(year), q,
                                     str(item["name"]), entity_type,
                                     item["registeredUsers"]))
    sql = "INSERT INTO top_user (state,year,quarter,entity_name,entity_type,registered_users) VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    print(f"✅ top_user: {len(rows)} rows loaded.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    clone_repo()
    try:
        conn = get_connection()
        create_database(conn)
        load_aggregated_transaction(conn)
        load_aggregated_user(conn)
        load_map_transaction(conn)
        load_map_user(conn)
        load_top_transaction(conn)
        load_top_user(conn)
        print("\n🎉 ETL complete! All data loaded into MySQL.")
    except Error as e:
        print(f"❌ DB Error: {e}")
    finally:
        if conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()
