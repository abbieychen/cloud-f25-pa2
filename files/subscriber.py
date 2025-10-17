#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from kafka import KafkaConsumer
import sqlite3
import os
from datetime import datetime


# Configuration from environment variables with defaults
BROKER_LIST = os.getenv('BROKER_LIST', 'localhost:9092')
TOPIC = os.getenv('TOPIC', 'weather')
RATE = float(os.getenv('RATE', '1.0'))
TOPIC_PREFIX = os.getenv('TOPIC_PREFIX', 'sensor')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SensorDataSubscriber:
    def __init__(self, broker_list, topics, db_path="/data/sensor_data.db"):
        self.broker_list = broker_list
        self.topics = topics
        self.db_path = db_path
        self.consumer = None
        self.setup_database()
    
    def setup_database(self):
        """Setup SQLite database and tables"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create table for sensor data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create index for better query performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_topic_sensor 
                ON sensor_data(topic, sensor_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON sensor_data(timestamp)
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"Database setup completed: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def connect(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.broker_list,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='sensor-data-consumer'
            )
            logger.info(f"Connected to Kafka brokers: {self.broker_list}")
            logger.info(f"Subscribed to topics: {self.topics}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def store_data(self, topic, data):
        """Store sensor data in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO sensor_data (topic, sensor_id, data_json, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (topic, data.get('sensor_id', 'unknown'), json.dumps(data), 
                  data.get('timestamp', datetime.utcnow().isoformat())))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store data: {e}")
            return False
    
    def process_messages(self):
        """Process incoming Kafka messages"""
        if not self.consumer:
            logger.error("Not connected to Kafka")
            return
        
        message_count = 0
        logger.info("Starting to consume messages...")
        
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                # Store in database
                if self.store_data(topic, data):
                    message_count += 1
                    
                    if message_count % 10 == 0:
                        logger.info(f"Processed {message_count} messages. Latest: {topic} from {data.get('sensor_id', 'unknown')}")
                
        except KeyboardInterrupt:
            logger.info("Subscriber stopped by user")
        except Exception as e:
            logger.error(f"Error during message processing: {e}")
        finally:
            self.consumer.close()
            logger.info(f"Total messages processed: {message_count}")

def main():
    parser = argparse.ArgumentParser(description='Kafka Sensor Data Subscriber')
    parser.add_argument('--broker-list', default=BROKER_LIST, help='Kafka broker list')
    parser.add_argument('--topic', default=TOPIC, choices=['weather', 'humidity', 'air-quality', 'power'],
                       help='Sensor topic to publish')
    parser.add_argument('--db-path', default='/data/sensor_data.db', help='SQLite database path')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Parse topics (support both comma-separated and wildcard)
    if ',' in args.topics:
        topics = [topic.strip() for topic in args.topics.split(',')]
    else:
        topics = [args.topics]
    
    # Create subscriber
    subscriber = SensorDataSubscriber(
        broker_list=args.broker_list.split(','),
        topics=topics,
        db_path=args.db_path
    )
    
    # Connect to Kafka
    if not subscriber.connect():
        sys.exit(1)
    
    # Start consuming
    subscriber.process_messages()

if __name__ == "__main__":
    main()