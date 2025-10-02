import pandas as pd
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from mongo_helpers import get_mongo_client, retry_on_transient_errors
import os

DATA_FILE = "online_retail.csv"
N_INVOICES = 1000  # number of invoices to ingest (min)

def load_csv(n_rows):
    df = pd.read_csv(DATA_FILE, encoding='unicode_escape', nrows=n_rows*3)  # overfetch slightly
    return df

@retry_on_transient_errors()
def upsert_product(coll_products, product_doc, session=None):
    coll_products.update_one(
        {"_id": product_doc["_id"]},
        {"$set": {"description": product_doc["description"]}},
        upsert=True,
        session=session
    )

@retry_on_transient_errors()
def upsert_customer(coll_customers, customer_doc, session=None):
    coll_customers.update_one(
        {"_id": customer_doc["_id"]},
        {"$set": {"country": customer_doc["country"]}},
        upsert=True,
        session=session
    )

def run(uri=None):
    client = get_mongo_client(uri)
    print(client)
    db = client["online_retail"]
    coll_customers = db.get_collection("customers")
    coll_products = db.get_collection("products")
    coll_invoices = db.get_collection("invoices")
    coll_invoice_items = db.get_collection("invoice_items")

    # set write concern for transactional safety if desired
    # coll_invoices = coll_invoices.with_options(write_concern=WriteConcern("majority"))

    df = load_csv(N_INVOICES)
    # drop rows lacking required keys
    df = df.dropna(subset=['InvoiceNo', 'CustomerID', 'StockCode', 'Description'])

    # Group rows by InvoiceNo -> each invoice will contain multiple line items
    grouped = df.groupby('InvoiceNo')
    processed = 0

    for invoice_no, group in grouped:
        if processed >= N_INVOICES:
            break

        invoice_header = {
            "_id": str(invoice_no),
            "invoiceDate": str(group['InvoiceDate'].iloc[0]),
            "customerId": str(group['CustomerID'].iloc[0])
        }

        items_docs = []
        # create items docs
        for _, row in group.iterrows():
            item = {
                "invoiceNo": str(invoice_no),
                "stockCode": str(row['StockCode']),
                "quantity": int(row['Quantity']) if not pd.isna(row['Quantity']) else 0,
                "unitPrice": float(row['UnitPrice']) if not pd.isna(row['UnitPrice']) else 0.0
            }
            items_docs.append(item)
        # products and customer upserts + invoice + items insertion in a transaction
        try:
            with client.start_session() as session:
                # start transaction
                with session.start_transaction():
                    # upsert customer
                    upsert_customer(coll_customers, {"_id": invoice_header["customerId"], "country": str(group['Country'].iloc[0])}, session=session)
                    # upsert all products for this invoice
                    for it in items_docs:
                        upsert_product(coll_products, {"_id": it["stockCode"], "description": str(group[group['StockCode'] == it["stockCode"]]['Description'].iloc[0])}, session=session)
                    # insert invoice header
                    coll_invoices.insert_one(invoice_header, session=session)
                    # insert items
                    coll_invoice_items.insert_many(items_docs, ordered=True, session=session)
            processed += 1
            if processed % 100 == 0:
                print(f"Inserted {processed} invoices (transactional)")
        except PyMongoError as e:
            print(f"Failed to insert invoice {invoice_no}: {e}")
            # transaction will abort on exception; continue with next invoice

    print(f"Done. Inserted {processed} transactional invoices.")
    client.close()

if __name__ == "__main__":
    import os
    uri = os.environ.get("MONGO_URI", None)
    run(uri=uri)

