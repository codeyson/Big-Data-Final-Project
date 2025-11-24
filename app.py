"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
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
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """
    STUDENT TODO: Configure sidebar settings and controls
    Implement any configuration options students might need
    """
    st.sidebar.title("Dashboard Controls")
    
    # STUDENT TODO: Add configuration options for data sources
    st.sidebar.subheader("Data Source Configuration")
    
    # Placeholder for Kafka configuration
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="STUDENT TODO: Configure your Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="streaming-data",
        help="STUDENT TODO: Specify the Kafka topic to consume from"
    )
    
    # Placeholder for storage configuration
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS", "MongoDB"],
        help="STUDENT TODO: Choose your historical data storage solution"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

def generate_sample_data():
    """
    STUDENT TODO: Replace this with actual data processing
    
    This function generates sample data for demonstration purposes.
    Students should replace this with real data from Kafka and storage systems.
    """
    # Sample data for demonstration - REPLACE WITH REAL DATA
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
    """
    Improved Kafka consumer with better timeout handling and debugging
    """
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    
    # Cache Kafka consumer
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # FIXED: Increase consumer timeout and add unique group_id
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,  # Changed to True for automatic offset management
                    group_id='streamlit-dashboard-' + str(int(time.time())),  # Unique group ID
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=15000,  # Increased to 15 seconds
                    max_poll_records=100  # Fetch more records at once
                )
                st.success(f"✅ Connected to Kafka: {kafka_broker} on topic: {kafka_topic}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Kafka connection attempt {attempt+1} failed: {e}. Retrying...")
                    time.sleep(retry_delay)
                else:
                    st.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                    st.session_state[cache_key] = None
    
    consumer = st.session_state[cache_key]

    if consumer is None:
        st.error("Unable to connect to Kafka. Using sample data.")
        return generate_sample_data()

    try:
        messages = []
        start_time = time.time()
        poll_timeout = 20  # Increased to 20 seconds
        
        # Debug info
        st.info(f"🔍 Polling Kafka for up to {poll_timeout} seconds...")

        while time.time() - start_time < poll_timeout:
            msg_pack = consumer.poll(timeout_ms=10000, max_records=100)  # Increased timeout
            
            if not msg_pack:
                # No messages in this poll
                time.sleep(1)
                continue

            for tp, batch in msg_pack.items():
                st.info(f"📦 Received {len(batch)} messages from partition {tp.partition}")
                
                for record in batch:
                    data = record.value
                    if data is None:
                        continue

                    # Handle LIST messages (your producer sends lists)
                    if isinstance(data, list):
                        for item in data:
                            if all(k in item for k in ['timestamp','value','metric_type','sensor_id']):
                                try:
                                    ts = item['timestamp']
                                    # Fix ISO 8601 format
                                    if ts.endswith("Z"):
                                        ts = ts[:-1] + "+00:00"
                                    timestamp = datetime.fromisoformat(ts)

                                    messages.append({
                                        'timestamp': timestamp,
                                        'value': float(item['value']),
                                        'metric_type': item['metric_type'],
                                        'sensor_id': item['sensor_id']
                                    })
                                except Exception as e:
                                    st.warning(f"⚠️ Error parsing list-item: {e}")
                        continue

                    # Handle SINGLE message
                    if all(k in data for k in ['timestamp','value','metric_type','sensor_id']):
                        try:
                            ts = data['timestamp']
                            if ts.endswith("Z"):
                                ts = ts[:-1] + "+00:00"
                            timestamp = datetime.fromisoformat(ts)

                            messages.append({
                                'timestamp': timestamp,
                                'value': float(data['value']),
                                'metric_type': data['metric_type'],
                                'sensor_id': data['sensor_id']
                            })
                        except Exception as e:
                            st.warning(f"⚠️ Error parsing message: {e}")
                    else:
                        st.warning(f"⚠️ Invalid message format: {data}")
            
            # If we got messages, we can break early
            if messages:
                break

        # Return messages or fallback sample
        if messages:
            st.success(f"✅ Successfully received {len(messages)} records from Kafka")
            return pd.DataFrame(messages)
        else:
            st.warning("⚠️ No Kafka messages received. Possible causes:\n"
                      "- Producer not running\n"
                      "- Wrong topic name\n"
                      "- Producer sending too slowly (check --rate parameter)\n"
                      "- Kafka broker not accessible")
            return generate_sample_data()

    except Exception as e:
        st.error(f"❌ Kafka consumer error: {e}. Using sample data.")
        import traceback
        st.code(traceback.format_exc())
        return generate_sample_data()






def query_historical_data(time_range="1h", metrics=None):
    """
    STUDENT TODO: Implement actual historical data query
    
    This function should:
    1. Connect to HDFS/MongoDB
    2. Query historical data based on time range and selected metrics
    3. Return aggregated historical data
    
    Parameters:
    - time_range: time period to query (e.g., "1h", "24h", "7d")
    - metrics: list of metric types to include
    
    Expected return format:
    pandas DataFrame with historical data
    """
    # STUDENT TODO: Replace with actual storage query
    st.warning("STUDENT TODO: Implement historical data query in query_historical_data() function")
    
    # Return sample data for template demonstration
    return generate_sample_data()


def display_real_time_view(config, refresh_interval):
    """
    Real-time Streaming View with multi-axis chart (fixed yaxis3 position).
    """
    st.header("📈 Real-time Streaming Dashboard")

    # Refresh status
    refresh_state = st.session_state.refresh_state
    st.info(
        f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} "
        f"- Updates every {refresh_interval} seconds"
    )

    # Kafka load
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)

        # Keep full history of all received Kafka messages in session state
        if "full_data" not in st.session_state:
            st.session_state.full_data = pd.DataFrame(columns=["timestamp", "value", "metric_type", "sensor_id"])

        # If consumer returned a DataFrame, ensure proper dtypes and append
        if real_time_data is not None and not real_time_data.empty:
            # Ensure timestamp column is datetime
            if not pd.api.types.is_datetime64_any_dtype(real_time_data["timestamp"]):
                try:
                    real_time_data["timestamp"] = pd.to_datetime(real_time_data["timestamp"])
                except Exception:
                    # fallback: keep as-is
                    pass

            st.session_state.full_data = pd.concat(
                [st.session_state.full_data, real_time_data],
                ignore_index=True
            )

            # Drop exact duplicates (same timestamp, value, metric_type, sensor_id)
            if not st.session_state.full_data.empty:
                st.session_state.full_data = st.session_state.full_data.drop_duplicates(subset=["timestamp", "value", "metric_type", "sensor_id"])
            
        # Use full data for plotting (may contain historical + live)
        full_data = st.session_state.full_data.copy()

    # If using sample fallback (generate_sample_data) the variable real_time_data may be non-empty;
    # prefer evaluating based on full_data so charts persist across refreshes.
    if full_data is not None and not full_data.empty:

        # Update last_refresh time for freshness calculation
        # (Only update last_refresh when we actually received new data in this run)
        if real_time_data is not None and not real_time_data.empty:
            st.session_state.refresh_state['last_refresh'] = datetime.now()

        # Data freshness
        data_freshness = datetime.now() - refresh_state["last_refresh"]
        freshness_color = (
            "🟢" if data_freshness.total_seconds() < 10
            else "🟡" if data_freshness.total_seconds() < 30
            else "🔴"
        )
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")

        # Quick metrics (use full_data so metrics reflect all stored points)
        st.subheader("📊 Live Data Metrics")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Records Received", len(full_data))
        with col2:
            try:
                st.metric("Latest Value", f"{full_data['value'].iloc[-1]:.2f}")
            except Exception:
                st.metric("Latest Value", "N/A")
        with col3:
            try:
                st.metric(
                    "Data Range",
                    f"{full_data['timestamp'].min().strftime('%H:%M')} - "
                    f"{full_data['timestamp'].max().strftime('%H:%M')}"
                )
            except Exception:
                st.metric("Data Range", "N/A")

        # ---------------------------- #
        # MULTI-AXIS PLOT (FIXED AXIS POSITIONS)
        # ---------------------------- #
        st.subheader("📈 Real-time Trend")

        fig = go.Figure()

        # Temperature
        temp = full_data[full_data["metric_type"] == "temperature"]
        if not temp.empty:
            fig.add_trace(go.Scatter(
                x=temp["timestamp"], y=temp["value"],
                mode="lines+markers", name="Temperature (°C)",
                yaxis="y1"
            ))

        # Humidity
        hum = full_data[full_data["metric_type"] == "humidity"]
        if not hum.empty:
            fig.add_trace(go.Scatter(
                x=hum["timestamp"], y=hum["value"],
                mode="lines+markers", name="Humidity (%)",
                yaxis="y2"
            ))

        # Pressure
        pres = full_data[full_data["metric_type"] == "pressure"]
        if not pres.empty:
            fig.add_trace(go.Scatter(
                x=pres["timestamp"], y=pres["value"],
                mode="lines+markers", name="Pressure (hPa)",
                yaxis="y3"
            ))

        # Layout with valid axis ranges
        fig.update_layout(
            title="Real-time Streaming Metrics",
            xaxis=dict(title="Time"),

            # Temperature axis (left)
            yaxis=dict(
                title="Temperature (°C)",
                side="left",
                color="green"
            ),

            # Humidity axis (far right)
            yaxis2=dict(
                title="Humidity (%)",
                overlaying="y",
                side="right",
                position=1.0,
                color="blue"
            ),

            # Pressure axis (slightly left of humidity)
            yaxis3=dict(
                title="Pressure (hPa)",
                overlaying="y",
                side="right",
                position=0.93,    # FIXED: must be between 0 and 1
                color="red"
            ),

            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02
            ),
            template="plotly_white"
        )

        st.plotly_chart(fig, width="stretch")

        # Raw data (show full_data so user sees accumulated points)
        with st.expander("📋 View Raw Data"):
            st.dataframe(
                full_data.sort_values("timestamp", ascending=False),
                width='stretch',
                height=300
            )

    else:
        st.warning("⚠️ No real-time data available from Kafka.")


def display_historical_view(config):
    """
    STUDENT TODO: Implement historical data query and visualization
    """
    st.header("📊 Historical Data Analysis")
    
    with st.expander("ℹ️ Implementation Guide"):
        st.info("""
        **STUDENT TODO:** This page should display historical data queried from HDFS or MongoDB.
        Implement the following:
        - Connection to your chosen storage system (HDFS/MongoDB)
        - Interactive filters and selectors for data exploration
        - Data aggregation and analysis capabilities
        - Historical trend visualization
        """)
    
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            help="STUDENT TODO: Implement time-based filtering in your query"
        )
    
    with col2:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "humidity", "pressure", "all"],
            help="STUDENT TODO: Implement metric filtering in your query"
        )
    
    with col3:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"],
            help="STUDENT TODO: Implement data aggregation in your query"
        )
    
    # STUDENT TODO: Replace with actual historical data query
    historical_data = query_historical_data(time_range, [metric_type] if metric_type != "all" else None)
    
    if historical_data is not None:
        # Display raw data
        st.subheader("Historical Data Table")
        st.info("STUDENT TODO: Customize data display for your specific dataset")
        
        st.dataframe(
            historical_data,
            width='stretch',
            hide_index=True
        )
        
        # Historical trends
        st.subheader("Historical Trends")
        st.info("STUDENT TODO: Implement meaningful historical analysis and visualization")
        
        if not historical_data.empty:
            # STUDENT TODO: Customize this analysis for your data
            fig = px.line(
                historical_data,
                x='timestamp',
                y='value',
                title="STUDENT TODO: Customize historical trend analysis"
            )
            st.plotly_chart(fig, width='stretch')
            
            # Additional analysis
            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Records", len(historical_data))
                st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}")
            
            with col2:
                st.metric("Average Value", f"{historical_data['value'].mean():.2f}")
                st.metric("Data Variability", f"{historical_data['value'].std():.2f}")
    
    else:
        st.error("STUDENT TODO: Historical data query not implemented")




def main():
    """
    STUDENT TODO: Customize the main application flow as needed
    """
    st.title("🚀 Streaming Data Dashboard")
    
    with st.expander("📋 Project Instructions"):
        st.markdown("""
        **STUDENT PROJECT TEMPLATE**
        
        ### Implementation Required:
        - **Real-time Data**: Connect to Kafka and process streaming data
        - **Historical Data**: Query from HDFS/MongoDB
        - **Visualizations**: Create meaningful charts
        - **Error Handling**: Implement robust error handling
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    else:
        # If auto_refresh disabled, ensure refresh_interval is defined for function calls
        refresh_interval = 15
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()
