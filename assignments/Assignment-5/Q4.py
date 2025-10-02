import json
import os
from pymongo import MongoClient

def create_config_file(output_path="config/atlas_config.json"):
    """
    Connects to MongoDB (Atlas or local), inspects server info,
    and writes a JSON config file describing the cluster + schema.
    """
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI not set. Please export it before running.")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    # get cluster/server info
    server_info = client.server_info()
    dbs = client.list_database_names()

    config = {
        "clusterName": "Cluster0",
        "provider": "AWS",
        "region": "ap-south-1 (Mumbai)",
        "tier": "M0 (Free Tier)",
        "replication": {
            "type": "Replica Set",
            "nodes": len(client.nodes)
        },
        "mongodbVersion": server_info.get("version"),
        "database": "online_retail",
        "collections": {
            "customers": "customer metadata",
            "products": "product catalog",
            "invoices": "invoice headers",
            "invoice_items": "line items"
        },
        "schemaModel": "transaction-centric"
    }

    # ensure folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(config, f, indent=4)

    print(f"Config file written to {output_path}")
    client.close()


if __name__ == "__main__":
    create_config_file()

