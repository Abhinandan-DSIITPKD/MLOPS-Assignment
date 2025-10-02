import pandas as pd
from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from mongo_helpers import get_mongo_client, retry_on_transient_errors

DATA_FILE = "online_retail.csv"
N_INVOICES = 1000

def load_csv(n_rows):
    df = pd.read_csv(DATA_FILE, encoding='unicode_escape', nrows=n_rows*3)
    return df

@retry_on_transient_errors()
def push_invoice_for_customer(coll_customers, customer_id, customer_country, invoice_doc):
    # Upsert the customer and push the invoice into the invoices array
    coll_customers.update_one(
        {"_id": customer_id},
        {
            "$setOnInsert": {"country": customer_country},
            "$push": {"invoices": invoice_doc}
        },
        upsert=True
    )

def run(uri=None):
    client = get_mongo_client(uri)
    db = client["online_retail"]
    coll_customers = db.get_collection("customers_cc")  # customer-centric collection

    df = load_csv(N_INVOICES)
    df = df.dropna(subset=['InvoiceNo', 'CustomerID', 'StockCode', 'Description'])

    grouped = df.groupby('InvoiceNo')
    processed = 0

    for invoice_no, group in grouped:
        if processed >= N_INVOICES:
            break

        customer_id = str(group['CustomerID'].iloc[0])
        customer_country = str(group['Country'].iloc[0])

        invoice_doc = {
            "invoiceNo": str(invoice_no),
            "invoiceDate": str(group['InvoiceDate'].iloc[0]),
            "items": []
        }
        for _, row in group.iterrows():
            item = {
                "stockCode": str(row['StockCode']),
                "description": str(row['Description']),
                "quantity": int(row['Quantity']) if not pd.isna(row['Quantity']) else 0,
                "unitPrice": float(row['UnitPrice']) if not pd.isna(row['UnitPrice']) else 0.0
            }
            invoice_doc["items"].append(item)

        try:
            push_invoice_for_customer(coll_customers, customer_id, customer_country, invoice_doc)
            processed += 1
            if processed % 100 == 0:
                print(f"Inserted {processed} invoices (customer-centric)")
        except PyMongoError as e:
            print(f"Failed inserting invoice {invoice_no} into customer {customer_id}: {e}")

    print(f"Done. Inserted {processed} invoices into customer-centric collection.")
    client.close()

if __name__ == "__main__":
    import os
    uri = os.environ.get("MONGO_URI", None)
    run(uri)

