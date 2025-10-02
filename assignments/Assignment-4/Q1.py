#!/usr/bin/env python3
import sqlite3
import time
import pandas as pd
import numpy as np

# --- Data Loading Configuration (Inlined) ---
DATA_FILE = "online_retail.csv"
N_RECORDS = 1000  # Minimum record requirement
DB_NAME = "online_retail.db"


def load_raw_data(n_rows: int = N_RECORDS) -> pd.DataFrame:
    """
    Load raw CSV rows. Note: original code used nrows=n_rows * 2 (over-fetch).
    I kept the same behavior to preserve your logic.
    """
    df = pd.read_csv(DATA_FILE, encoding="unicode_escape", nrows=n_rows * 2)
    return df


def setup_db(conn: sqlite3.Connection) -> None:
    """Create normalized tables if they don't exist."""
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID TEXT PRIMARY KEY,
            Country TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Product (
            StockCode TEXT PRIMARY KEY,
            Description TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Invoice (
            InvoiceNo TEXT PRIMARY KEY,
            InvoiceDate TEXT,
            CustomerID TEXT,
            FOREIGN KEY (CustomerID) REFERENCES Customer (CustomerID)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS InvoiceItem (
            InvoiceNo TEXT,
            StockCode TEXT,
            Quantity INTEGER,
            UnitPrice REAL,
            PRIMARY KEY (InvoiceNo, StockCode),
            FOREIGN KEY (InvoiceNo) REFERENCES Invoice (InvoiceNo),
            FOREIGN KEY (StockCode) REFERENCES Product (StockCode)
        )
        """
    )

    conn.commit()


def insert_data(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert cleaned/filtered DataFrame rows into the database."""
    cursor = conn.cursor()

    # Filtering added here to prevent foreign key errors with raw data
    df_filtered = df.dropna(subset=["CustomerID", "Description", "InvoiceNo"]).copy()

    # --- 1. Customer Data ---
    customers = df_filtered[["CustomerID", "Country"]].drop_duplicates().values.tolist()
    cursor.executemany(
        "INSERT OR IGNORE INTO Customer (CustomerID, Country) VALUES (?, ?)",
        customers,
    )

    # --- 2. Product Data ---
    products = df_filtered[["StockCode", "Description"]].drop_duplicates().values.tolist()
    cursor.executemany(
        "INSERT OR IGNORE INTO Product (StockCode, Description) VALUES (?, ?)",
        products,
    )

    # --- 3. Invoice Data ---
    invoices = df_filtered[["InvoiceNo", "InvoiceDate", "CustomerID"]].drop_duplicates().values.tolist()
    cursor.executemany(
        "INSERT OR IGNORE INTO Invoice (InvoiceNo, InvoiceDate, CustomerID) VALUES (?, ?, ?)",
        invoices,
    )

    # --- 4. InvoiceItem Data (Line Items) ---
    items = df_filtered[["InvoiceNo", "StockCode", "Quantity", "UnitPrice"]].values.tolist()
    cursor.executemany(
        "INSERT OR IGNORE INTO InvoiceItem (InvoiceNo, StockCode, Quantity, UnitPrice) VALUES (?, ?, ?, ?)",
        items,
    )

    conn.commit()

    print("\n--- SQL Data Load Complete ---")
    print(f"Customers: {len(customers)}, Products: {len(products)}, Invoices: {len(invoices)}")


if __name__ == "__main__":
    # 1. Load Data
    data_df = load_raw_data(n_rows=1000)
    if data_df.empty:
        print("No data loaded. Exiting.")
        exit()

    # 2. Connect and Setup DB
    conn = sqlite3.connect(DB_NAME)

    # Ensure SQLite enforces foreign keys (recommended)
    conn.execute("PRAGMA foreign_keys = ON")

    setup_db(conn)

    # 3. Insert Data
    insert_data(conn, data_df)

    conn.close()

