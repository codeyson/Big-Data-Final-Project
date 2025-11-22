# app.py
"""
Streaming Data Dashboard with MongoDB historical storage support
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# Import the MongoDB helper (new file)
try:
    import storage_mongo
    MONGO_AVAILABLE = True
except Exception:
    storage_mongo = None
    MONGO_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("Data Source Configuration")

    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="streaming-data",
        help="Kafka topic to consume from"
    )
    
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS", "MongoDB"],
        help="Choose your historical data storage solution"
    )
    
    # MongoDB connection (shown when MongoDB is selected)
    mongo_uri = None
    if storage_type == "MongoDB":
        if not MONGO_AVAILABLE:
            st.sidebar.error("pymongo (storage_mongo) not available. Install pymongo.")
        mongo_uri = st.sidebar.text_input(
            "MongoDB URI",
            value="mongodb://localhost:27017",
            help="MongoDB connection URI (used for historical storage). Can set env MONGO_URI instead."
        )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type,
        "mongo_uri": mongo_uri
    }

def generate_sample_data():
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['sensor_1'] * 100
    })
    return sample_data

def consume_kafka_data(config):
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    storage_type = config.get("storage_type", None)
    mongo_uri = config.get("mongo_uri", None)

    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        max_retries = 3
        retry_delay = 2  # seconds
        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=2000
                )
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Kafka connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    st.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                    st.session_state[cache_key] = None

    consumer = st.session_state[cache_key]
    if consumer:
        try:
            messages = []
            start_time = time.time()
            poll_timeout = 3
            while time.time() - start_time < poll_timeout and len(messages) < 50:
                msg_pack = consumer.poll(timeout_ms=500)
                for tp, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        try:
                            data = message.value
                            if all(key in data for key in ['timestamp', 'value', 'metric_type', 'sensor_id']):
                                timestamp_str = data['timestamp']
                                try:
                                    if isinstance(timestamp_str, str) and timestamp_str.endswith('Z'):
                                        timestamp_str = timestamp_str[:-1] + '+00:00'
                                    timestamp = datetime.fromisoformat(timestamp_str) if isinstance(timestamp_str, str) else timestamp_str
                                    messages.append({
                                        'timestamp': timestamp,
                                        'value': float(data['value']),
                                        'metric_type': data['metric_type'],
                                        'sensor_id': data['sensor_id'],
                                        # keep extra fields
                                        **{k: v for k, v in data.items() if k not in ['timestamp', 'value', 'metric_type', 'sensor_id']}
                                    })
                                except ValueError:
                                    st.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")
                            else:
                                st.warning(f"Invalid message format: {data}")
                        except Exception as e:
                            st.warning(f"Error processing message: {e}")

            if messages:
                # If user chose MongoDB, write messages to MongoDB for historical storage (best-effort)
                if storage_type == "MongoDB":
                    if not MONGO_AVAILABLE:
                        st.warning("pymongo not installed; cannot write to MongoDB. Install pymongo to enable historical storage.")
                    else:
                        try:
                            # convert datetimes to ISO strings for insertion helper (it accepts datetime or ISO)
                            insert_count = storage_mongo.insert_records(
                                db_name=storage_mongo.DEFAULT_DB,
                                collection_name=storage_mongo.DEFAULT_COLLECTION,
                                records=messages,
                                mongo_uri=mongo_uri
                            )
                            st.info(f"Inserted {insert_count} record(s) into MongoDB for historical storage.")
                        except Exception as e:
                            st.warning(f"Failed to insert records into MongoDB: {e}")

                # convert to DataFrame for display
                df = pd.DataFrame(messages)
                # ensure timestamp dtype
                if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            else:
                st.info("No messages received from Kafka. Using sample data.")
                return generate_sample_data()
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            st.error(f"Kafka error: {e}. Using sample data.")
            return generate_sample_data()
    else:
        st.error("Unable to connect to Kafka. Using sample data.")
        return generate_sample_data()

def query_historical_data(time_range="1h", metrics=None, aggregation="raw", mongo_uri=None):
    """
    Query historical data from MongoDB when available. Falls back to sample data if MongoDB not configured.
    """
    if not MONGO_AVAILABLE:
        st.warning("pymongo not installed. Cannot query historical MongoDB. Returning sample data.")
        return generate_sample_data()

    try:
        docs = storage_mongo.query_records(
            db_name=storage_mongo.DEFAULT_DB,
            collection_name=storage_mongo.DEFAULT_COLLECTION,
            time_range=time_range,
            metric_types=metrics,
            aggregation=aggregation,
            limit=2000,
            mongo_uri=mongo_uri
        )

        if docs is None or len(docs) == 0:
            st.info("No historical records found in MongoDB for the selected filters. Returning sample data.")
            return generate_sample_data()

        # When aggregation == 'raw', docs are documents with timestamp/value fields. When aggregated, docs contain avg/min/max/count.
        if aggregation == "raw":
            df = pd.DataFrame(docs)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            # Keep only expected display columns if present
            cols = [c for c in ['timestamp', 'value', 'metric_type', 'sensor_id'] if c in df.columns]
            return df[cols] if cols else df
        else:
            # aggregated docs
            rows = []
            for d in docs:
                _id = d.get("_id", {})
                bucket = _id.get("bucket") or d.get("timestamp") or d.get("timestamp")
                metric_type = _id.get("metric_type") if isinstance(_id, dict) else None
                rows.append({
                    "timestamp": d.get("timestamp") or bucket,
                    "metric_type": metric_type,
                    "avg_value": d.get("avg_value") or d.get("avg_value"),
                    "min_value": d.get("min_value"),
                    "max_value": d.get("max_value"),
                    "count": d.get("count", 1)
                })
            df = pd.DataFrame(rows)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df

    except Exception as e:
        st.warning(f"Error querying MongoDB: {e}. Returning sample data.")
        return generate_sample_data()

def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")

    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)

    if real_time_data is not None:
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")

        st.subheader("📊 Live Data Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            with col3:
                st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")

        st.subheader("📈 Real-time Trend")
        if not real_time_data.empty:
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Sensor Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(xaxis_title="Time", yaxis_title="Value", hovermode='x unified')
            st.plotly_chart(fig, width='stretch')

            with st.expander("📋 View Raw Data"):
                st.dataframe(real_time_data.sort_values('timestamp', ascending=False), width='stretch', height=300)
        else:
            st.warning("No real-time data available. Check Kafka topic/producer.")
    else:
        st.error("No data available")

def display_historical_view(config):
    st.header("📊 Historical Data Analysis")
    with st.expander("ℹ️ Implementation Guide"):
        st.info("""
        This page displays historical data queried from MongoDB.
        Use the controls to select time range, metric type and aggregation.
        """)

    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)
    with col1:
        time_range = st.selectbox("Time Range", ["1h", "24h", "7d", "30d"])
    with col2:
        metric_type = st.selectbox("Metric Type", ["all", "temperature", "humidity", "pressure"])
    with col3:
        aggregation = st.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])

    metrics = None if metric_type == "all" else [metric_type]

    mongo_uri = config.get("mongo_uri", None) if config.get("storage_type") == "MongoDB" else None
    historical_data = query_historical_data(time_range, metrics, aggregation, mongo_uri=mongo_uri)

    if historical_data is not None:
        st.subheader("Historical Data Table")
        st.dataframe(historical_data, width='stretch', hide_index=True)

        st.subheader("Historical Trends")
        if not historical_data.empty:
            if aggregation == "raw":
                fig = px.line(historical_data, x='timestamp', y='value', color='metric_type' if 'metric_type' in historical_data.columns else None,
                              title="Historical raw data")
            else:
                # aggregated
                y = 'avg_value' if 'avg_value' in historical_data.columns else (historical_data.columns[1] if len(historical_data.columns) > 1 else historical_data.columns[0])
                fig = px.line(historical_data, x='timestamp', y=y, color='metric_type' if 'metric_type' in historical_data.columns else None,
                              title=f"Historical ({aggregation})")
            st.plotly_chart(fig, width='stretch')

            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", len(historical_data))
                try:
                    drange = f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}"
                except Exception:
                    drange = "N/A"
                st.metric("Date Range", drange)
            with col2:
                # show average if available
                if 'avg_value' in historical_data.columns:
                    st.metric("Average (agg)", f"{historical_data['avg_value'].mean():.2f}")
                elif 'value' in historical_data.columns:
                    st.metric("Average Value", f"{historical_data['value'].mean():.2f}")
                else:
                    st.metric("Average Value", "N/A")
    else:
        st.error("Historical query failed or returned no data")

def main():
    st.title("🚀 Streaming Data Dashboard (MongoDB)")

    with st.expander("📋 Project Instructions"):
        st.markdown("""
        - Real-time Data: Connect to Kafka and process streaming data
        - Historical Data: Query from MongoDB
        - Visualizations & Aggregations
        """)

    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'last_refresh': datetime.now(), 'auto_refresh': True}

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh']
    )

    refresh_interval = 15
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15)
        # Auto-refresh
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
