# benchmark_crud.py
import sqlite3
import time
import random
import pandas as pd
import numpy as np
from pymongo import MongoClient
from mongo_helpers import get_mongo_client
import statistics
import json
import os

# CONFIG
SQLITE_DB = "online_retail.db"  # the DB your assignment Q1 script created
MONGO_URI = None  # set to your Atlas URI string or None for localhost
NUM_ITER = 100  # number of operations per test (lower if slow)
SEED = 42
random.seed(SEED)

def sqlite_connect():
    return sqlite3.connect(SQLITE_DB)

def sqlite_get_random_invoice_numbers(conn, k):
    cur = conn.cursor()
    cur.execute("SELECT InvoiceNo FROM Invoice")
    all_ids = [row[0] for row in cur.fetchall()]
    return random.sample(all_ids, min(k, len(all_ids)))

def mongo_get_random_invoice_ids(db, k, coll_name):
    coll = db[coll_name]
    # aggregate to sample k ids
    ids = [doc['_id'] for doc in coll.find({}, {'_id': 1}).limit(10000)]
    return random.sample(ids, min(k, len(ids)))

def time_func(func, *a, **kw):
    t0 = time.perf_counter()
    func(*a, **kw)
    return time.perf_counter() - t0

def bench_sqlite_read_invoice(conn, invoice_no):
    cur = conn.cursor()
    # join invoice and invoice items
    cur.execute("SELECT Invoice.InvoiceNo, Invoice.InvoiceDate, Invoice.CustomerID, InvoiceItem.StockCode, InvoiceItem.Quantity, InvoiceItem.UnitPrice FROM Invoice LEFT JOIN InvoiceItem ON Invoice.InvoiceNo = InvoiceItem.InvoiceNo WHERE Invoice.InvoiceNo = ?", (invoice_no,))
    _ = cur.fetchall()

def bench_sqlite_insert(conn, invoice_no):
    cur = conn.cursor()
    # simple insert: insert invoice and one sample item (make sure keys don't collide)
    cur.execute("INSERT OR IGNORE INTO Invoice (InvoiceNo, InvoiceDate, CustomerID) VALUES (?, datetime('now'), '0')", (invoice_no,))
    cur.execute("INSERT OR IGNORE INTO InvoiceItem (InvoiceNo, StockCode, Quantity, UnitPrice) VALUES (?, 'SAMPLE', 1, 1.0)", (invoice_no,))
    conn.commit()

def bench_sqlite_update(conn, invoice_no):
    cur = conn.cursor()
    cur.execute("UPDATE InvoiceItem SET Quantity = Quantity + 1 WHERE InvoiceNo = ?", (invoice_no,))
    conn.commit()

def bench_sqlite_delete(conn, invoice_no):
    cur = conn.cursor()
    cur.execute("DELETE FROM InvoiceItem WHERE InvoiceNo = ?", (invoice_no,))
    cur.execute("DELETE FROM Invoice WHERE InvoiceNo = ?", (invoice_no,))
    conn.commit()

# Mongo equivalents (transactional and customer-centric)
def bench_mongo_read_invoice_transactional(db, invoice_no):
    inv = db.invoices.find_one({"_id": invoice_no})
    items = list(db.invoice_items.find({"invoiceNo": invoice_no}))
    return

def bench_mongo_insert_transactional(client, db, invoice_no):
    with client.start_session() as session:
        with session.start_transaction():
            db.invoices.insert_one({"_id": invoice_no, "invoiceDate": "now", "customerId": "0"}, session=session)
            db.invoice_items.insert_one({"invoiceNo": invoice_no, "stockCode": "SAMPLE", "quantity": 1, "unitPrice": 1.0}, session=session)

def bench_mongo_update_transactional(db, invoice_no):
    db.invoice_items.update_one({"invoiceNo": invoice_no}, {"$inc": {"quantity": 1}})

def bench_mongo_delete_transactional(client, db, invoice_no):
    with client.start_session() as session:
        with session.start_transaction():
            db.invoice_items.delete_many({"invoiceNo": invoice_no}, session=session)
            db.invoices.delete_one({"_id": invoice_no}, session=session)

def bench_mongo_read_customer_centric(db, customer_id):
    doc = db.customers_cc.find_one({"_id": customer_id})
    return

def bench_mongo_insert_customer_centric(db, customer_id, invoice_no):
    invoice_doc = {"invoiceNo": invoice_no, "invoiceDate": "now", "items": [{"stockCode": "SAMPLE", "quantity": 1, "unitPrice": 1.0}]}
    db.customers_cc.update_one({"_id": customer_id}, {"$setOnInsert": {"country": "XX"}, "$push": {"invoices": invoice_doc}}, upsert=True)

def bench_mongo_update_customer_centric(db, customer_id):
    # increment quantity of first item in first invoice (if exists)
    db.customers_cc.update_one({"_id": customer_id}, {"$inc": {"invoices.0.items.0.quantity": 1}})

def bench_mongo_delete_customer_centric(db, customer_id, invoice_no):
    db.customers_cc.update_one({"_id": customer_id}, {"$pull": {"invoices": {"invoiceNo": invoice_no}}})

def run_benchmarks():
    results = []
    # SQLite setup
    sql_conn = sqlite_connect()
    sql_invoice_ids = sqlite_get_random_invoice_numbers(sql_conn, NUM_ITER)
    # Mongo setup
    mongo_client = get_mongo_client(MONGO_URI)
    mdb = mongo_client["online_retail"]
    mongo_invoice_ids = mongo_get_random_invoice_ids(mdb, NUM_ITER, "invoices")
    mongo_customer_ids = [doc["_id"] for doc in mdb.customers_cc.find({}, {"_id": 1}).limit(NUM_ITER)]

    # READ SQLite
    for inv in sql_invoice_ids:
        t = time_func(bench_sqlite_read_invoice, sql_conn, inv)
        results.append({"system": "sqlite", "operation": "read_invoice", "time": t})
    # READ Mongo transactional
    for inv in mongo_invoice_ids:
        t = time_func(bench_mongo_read_invoice_transactional, mdb, inv)
        results.append({"system": "mongo_tx", "operation": "read_invoice", "time": t})
    # READ Mongo customer-centric
    for cid in mongo_customer_ids:
        t = time_func(bench_mongo_read_customer_centric, mdb, cid)
        results.append({"system": "mongo_cc", "operation": "read_customer", "time": t})

    # INSERT tests (use unique ids)
    for i in range(10):
        new_inv = f"NEW_SQL_{i}_{int(time.time()*1000)}"
        t = time_func(bench_sqlite_insert, sql_conn, new_inv)
        results.append({"system": "sqlite", "operation": "insert_invoice", "time": t})
    for i in range(10):
        new_inv = f"NEW_MTX_{i}_{int(time.time()*1000)}"
        t = time_func(bench_mongo_insert_transactional, mongo_client, mdb, new_inv)
        results.append({"system": "mongo_tx", "operation": "insert_invoice", "time": t})
    for i in range(10):
        cust = f"NEW_CUST_{i}_{int(time.time()*1000)}"
        inv = f"NEW_CC_{i}_{int(time.time()*1000)}"
        t = time_func(bench_mongo_insert_customer_centric, mdb, cust, inv)
        results.append({"system": "mongo_cc", "operation": "insert_invoice", "time": t})

    # small updates
    for inv in sql_invoice_ids[:20]:
        t = time_func(bench_sqlite_update, sql_conn, inv)
        results.append({"system": "sqlite", "operation": "update_item", "time": t})
    for inv in mongo_invoice_ids[:20]:
        t = time_func(bench_mongo_update_transactional, mdb, inv)
        results.append({"system": "mongo_tx", "operation": "update_item", "time": t})
    for cid in mongo_customer_ids[:20]:
        t = time_func(bench_mongo_update_customer_centric, mdb, cid)
        results.append({"system": "mongo_cc", "operation": "update_item", "time": t})

    # deletes
    # clean those NEW_* inserted earlier - skip for brevity

    # Save results
    df = pd.DataFrame(results)
    fname = "benchmark_results.csv"
    df.to_csv(fname, index=False)
    print(f"Saved {len(df)} rows to {fname}")
    sql_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    run_benchmarks()

