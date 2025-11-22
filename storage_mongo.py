# storage_mongo.py
"""
MongoDB helper for the Streaming Data Dashboard
Provides:
- get_mongo_client()
- insert_records(db_name, collection_name, records)
- query_records(db_name, collection_name, time_from, time_to, metric_types, aggregation)
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import os
import warnings

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import PyMongoError
except Exception:
    # If pymongo isn't installed, fail later with clear message from app
    MongoClient = None
    PyMongoError = Exception

DEFAULT_DB = "streaming_dashboard"
DEFAULT_COLLECTION = "measurements"

def get_mongo_client(mongo_uri: Optional[str] = None):
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed. Install it with `pip install pymongo`.")
    mongo_uri = mongo_uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    return MongoClient(mongo_uri)

def ensure_indexes(collection):
    try:
        collection.create_index([("timestamp", ASCENDING)])
        collection.create_index([("metric_type", ASCENDING)])
        collection.create_index([("sensor_id", ASCENDING)])
    except Exception:
        # best-effort
        pass

def insert_records(db_name: str, collection_name: str, records: List[Dict[str, Any]], mongo_uri: Optional[str] = None) -> int:
    """
    Insert a list of dict-records into MongoDB.
    Returns number of inserted documents.
    Each record should contain 'timestamp' (datetime or ISO string), 'value', 'metric_type', 'sensor_id'.
    """
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed. Install it with `pip install pymongo`.")
    if not records:
        return 0

    client = get_mongo_client(mongo_uri)
    db = client[db_name]
    coll = db[collection_name]
    ensure_indexes(coll)

    prepared = []
    for r in records:
        rec = r.copy()
        ts = rec.get("timestamp")
        if isinstance(ts, str):
            # Try to parse ISO string
            try:
                # handle trailing Z
                if ts.endswith("Z"):
                    ts = ts[:-1] + "+00:00"
                rec["timestamp"] = datetime.fromisoformat(ts)
            except Exception:
                # fallback - set to now
                rec["timestamp"] = datetime.utcnow()
        elif isinstance(ts, (int, float)):
            rec["timestamp"] = datetime.utcfromtimestamp(ts)
        elif not isinstance(ts, datetime):
            rec["timestamp"] = datetime.utcnow()
        prepared.append(rec)
    try:
        result = coll.insert_many(prepared, ordered=False)
        inserted = len(result.inserted_ids)
    except PyMongoError as e:
        warnings.warn(f"MongoDB insert error: {e}")
        inserted = 0
    finally:
        client.close()
    return inserted

def _build_time_range(time_range: str):
    """Returns (from_datetime, to_datetime) for a time_range string like '1h', '24h', '7d' or '30d'."""
    now = datetime.utcnow()
    if time_range.endswith("h"):
        hours = int(time_range[:-1])
        return now - timedelta(hours=hours), now
    if time_range.endswith("d"):
        days = int(time_range[:-1])
        return now - timedelta(days=days), now
    # default: 1 hour
    return now - timedelta(hours=1), now

def query_records(db_name: str,
                  collection_name: str,
                  time_range: str = "1h",
                  metric_types: Optional[List[str]] = None,
                  aggregation: str = "raw",
                  limit: int = 1000,
                  mongo_uri: Optional[str] = None):
    """
    Query historical records.
    aggregation: 'raw', 'hourly', 'daily', 'weekly'
    Returns a list of dict records or aggregated docs with fields: timestamp, value (avg), metric_type, sensor_id
    """
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed. Install it with `pip install pymongo`.")
    client = get_mongo_client(mongo_uri)
    db = client[db_name]
    coll = db[collection_name]

    time_from, time_to = _build_time_range(time_range)

    match_stage = {
        "$match": {
            "timestamp": {"$gte": time_from, "$lte": time_to}
        }
    }
    if metric_types:
        match_stage["$match"]["metric_type"] = {"$in": metric_types}

    if aggregation == "raw":
        pipeline = [
            match_stage,
            {"$sort": {"timestamp": 1}},
            {"$limit": limit}
        ]
        try:
            docs = list(coll.aggregate(pipeline))
        except PyMongoError:
            docs = list(coll.find(match_stage["$match"]).sort("timestamp", 1).limit(limit))
        client.close()
        return docs

    # For aggregated outputs (hourly/daily/weekly) we group by metric_type and a truncated timestamp
    if aggregation == "hourly":
        group_format = {"$dateToString": {"format": "%Y-%m-%dT%H:00:00Z", "date": "$timestamp"}}
    elif aggregation == "daily":
        group_format = {"$dateToString": {"format": "%Y-%m-%dT00:00:00Z", "date": "$timestamp"}}
    elif aggregation == "weekly":
        # group by year-week
        group_format = {"$dateToString": {"format": "%G-W%V", "date": "$timestamp"}}
    else:
        group_format = {"$dateToString": {"format": "%Y-%m-%dT%H:00:00Z", "date": "$timestamp"}}

    pipeline = [
        match_stage,
        {
            "$group": {
                "_id": {
                    "bucket": group_format,
                    "metric_type": "$metric_type"
                },
                "timestamp": {"$first": "$timestamp"},
                "avg_value": {"$avg": "$value"},
                "min_value": {"$min": "$value"},
                "max_value": {"$max": "$value"},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"timestamp": 1}},
        {"$limit": limit}
    ]

    try:
        docs = list(coll.aggregate(pipeline))
    except PyMongoError as e:
        # fallback: return raw documents
        docs = list(coll.find(match_stage["$match"]).sort("timestamp", 1).limit(limit))
    finally:
        client.close()
    return docs
