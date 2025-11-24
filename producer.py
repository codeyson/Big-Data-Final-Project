"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import argparse
import json
import time
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List
import requests


# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


class StreamingDataProducer:
    """
    Enhanced Kafka producer with stateful synthetic data generation
    This class handles Kafka connection, realistic data generation, and message sending
    """
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka producer configuration with stateful data generation
        
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            # 'security_protocol': 'SSL',  # If using SSL
            # 'ssl_cafile': 'path/to/ca.pem',  # If using SSL
            # 'ssl_certfile': 'path/to/service.cert',  # If using SSL
            # 'ssl_keyfile': 'path/to/service.key',  # If using SSL
            # 'compression_type': 'gzip',  # Optional: Enable compression
            # 'batch_size': 16384,  # Optional: Tune batch size
            # 'linger_ms': 10,  # Optional: Wait for batch fill
        }
        
        # Stateful data generation attributes
        self.sensor_states = {}  # Track state for each sensor
        self.time_counter = 0    # Track time progression
        self.base_time = datetime.utcnow()
        
        # Expanded sensor pool with metadata
        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_b", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_003", "location": "outdoor_north", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_004", "location": "lab_1", "type": "humidity", "unit": "percent"},
            {"id": "sensor_005", "location": "lab_2", "type": "humidity", "unit": "percent"},
            {"id": "sensor_006", "location": "control_room", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_007", "location": "factory_floor", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_008", "location": "warehouse", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_009", "location": "office_area", "type": "humidity", "unit": "percent"},
            {"id": "sensor_010", "location": "basement", "type": "pressure", "unit": "hPa"},
        ]
        
        # Metric type configurations
        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
        }
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_sample_data(self):
        """Fetch OpenWeather live data or generate synthetic."""
        try:
            API_KEY = "9c07e4455e6f428cd0ba1647139b58ca"
            CITY = "Manila"

            url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
            response = requests.get(url).json()

            if "main" in response:
                return [
                    {
                        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
                        "value": response["main"]["temp"],
                        "metric_type": "temperature",
                        "sensor_id": "weather_manila_temp",
                    },
                    {
                        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
                        "value": response["main"]["humidity"],
                        "metric_type": "humidity",
                        "sensor_id": "weather_manila_humidity",
                    },
                    {
                        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
                        "value": response["main"]["pressure"],
                        "metric_type": "pressure",
                        "sensor_id": "weather_manila_pressure",
                    },
                ]
        except Exception as e:
            print("OpenWeather API error:", e)

        # ---- ALWAYS RETURN A LIST OF DICTS ----
        return [
            {
                "timestamp": datetime.utcnow().isoformat() + "+00:00",
                "value": round(random.uniform(20, 30), 2),
                "metric_type": "temperature",
                "sensor_id": "sensor_fallback_temp",
            },
            {
                "timestamp": datetime.utcnow().isoformat() + "+00:00",
                "value": round(random.uniform(40, 70), 2),
                "metric_type": "humidity",
                "sensor_id": "sensor_fallback_humidity",
            },
            {
                "timestamp": datetime.utcnow().isoformat() + "+00:00",
                "value": round(random.uniform(990, 1020), 2),
                "metric_type": "pressure",
                "sensor_id": "sensor_fallback_pressure",
            },
        ]




    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        STUDENT TODO: Implement data serialization
        
        Convert the data dictionary to bytes for Kafka transmission.
        Common formats: JSON, Avro, Protocol Buffers
        
        For simplicity, we use JSON serialization in this template.
        Consider using Avro for better schema evolution in production.
        """
        
        # STUDENT TODO: Choose and implement your serialization method
        try:
            # JSON serialization (simple but less efficient)
            serialized_data = json.dumps(data).encode('utf-8')
            
            # STUDENT TODO: Consider using Avro for better performance and schema management
            # from avro import schema, datafile, io
            # serialized_data = avro_serializer.serialize(data)
            
            return serialized_data
        except Exception as e:
            print(f"STUDENT TODO: Implement proper error handling for serialization: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Implement message sending to Kafka
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if message was sent successfully, False otherwise
        """
        
        # Check if producer is initialized
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        
        # Serialize the data
        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False
        
        try:
            # Send message to Kafka
            future = self.producer.send(self.topic, value=serialized_data)
            # Wait for send confirmation with timeout
            result = future.get(timeout=10)
            print(f"Message sent successfully - Topic: {self.topic}, Data: {data}")
            return True
            
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):

        print(f"Starting producer: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals), duration: {duration or 'infinite'}")

        start_time = time.time()
        message_count = 0

        try:
            while True:

                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break

                # MUST loop through list of messages
                messages = self.generate_sample_data()
                for msg in messages:
                    success = self.send_message(msg)
                    if success:
                        message_count += 1

                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")


    def close(self):
        """Implement producer cleanup and resource release"""
        if self.producer:
            try:
                # Ensure all messages are sent
                self.producer.flush(timeout=10)
                # Close producer connection
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """STUDENT TODO: Configure command-line arguments for flexibility"""
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    
    # STUDENT TODO: Add additional command-line arguments as needed
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='streaming-data',
        help='Kafka topic to produce to (default: streaming-data)'
    )
    
    parser.add_argument(
        '--rate',
        type=float,
        default=0.1,
        help='Messages per second (default: 0.1 for 10-second intervals)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    # STUDENT TODO: Add more arguments for your specific use case
    # parser.add_argument('--sensor-count', type=int, default=5, help='Number of simulated sensors')
    # parser.add_argument('--data-type', choices=['temperature', 'humidity', 'financial'], default='temperature')
    
    return parser.parse_args()


def main():
    """
    STUDENT TODO: Customize the main execution flow as needed
    
    Implementation Steps:
    1. Parse command-line arguments
    2. Initialize Kafka producer
    3. Start streaming data
    4. Handle graceful shutdown
    """
    
    print("=" * 60)
    print("STREAMING DATA PRODUCER TEMPLATE")
    print("STUDENT TODO: Implement all sections marked with 'STUDENT TODO'")
    print("=" * 60)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Initialize producer
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    # Start producing stream
    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration
        )
    except Exception as e:
        print(f"STUDENT TODO: Handle main execution errors: {e}")
    finally:
        print("Producer execution completed")


# STUDENT TODO: Testing Instructions
if __name__ == "__main__":
    main()
