import time
import random
from functools import wraps
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure

def get_mongo_client(uri=None, max_pool_size=100, min_pool_size=0, server_selection_timeout_ms=5000, connect_timeout_ms=10000):
    
    if not uri:
        uri = "mongodb://localhost:27017"
        
    client = MongoClient(
        uri,
        maxPoolSize=max_pool_size,
        minPoolSize=min_pool_size,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        retryWrites=True
    )
    # Optionally: force server selection to raise early if can't connect
    client.admin.command('ping')
    return client

def retry_on_transient_errors(max_attempts=3, base_delay=0.5, backoff=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except (AutoReconnect, ConnectionFailure, ServerSelectionTimeoutError) as ex:
                    attempts += 1
                    if attempts >= max_attempts:
                        raise
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator

