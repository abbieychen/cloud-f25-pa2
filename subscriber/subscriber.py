#!/usr/bin/env python3
import json
import argparse
import os
import logging
from kafka import KafkaConsumer
import sqlite3
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensorDataSubscriber:
    def __init__(self, broker_list, topics, db_path):
        self.broker_list = broker_list
        self.topics = topics
        self.db_path = db_path
        self.consumer = None
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with sensor_data table"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
    
    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.broker_list,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='sensor-consumer-group'
            )
            logger.info(f"Connected to Kafka brokers: {self.broker_list}")
            logger.info(f"Subscribed to topics: {self.topics}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def store_data(self, topic, data):
        """Store sensor data in SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO sensor_data (topic, sensor_id, timestamp, data_json)
                VALUES (?, ?, ?, ?)
            ''', (topic, data.get('sensor_id', 'unknown'), 
                  data.get('timestamp', ''), json.dumps(data)))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Failed to store data in database: {e}")
            return False
    
    def consume(self):
        if not self.connect():
            return
        
        logger.info("Starting to consume messages...")
        try:
            for message in self.consumer:
                data = message.value
                topic = message.topic
                
                logger.info(f"Received from {topic}: {data}")
                
                # Store in database
                if self.store_data(topic, data):
                    logger.debug(f"Stored data from {topic} in database")
                else:
                    logger.error(f"Failed to store data from {topic}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            if self.consumer:
                self.consumer.close()

def main():
    parser = argparse.ArgumentParser(description='Sensor Data Subscriber')
    parser.add_argument('--topics', type=str, nargs='+', required=True,
                       help='Kafka topics to subscribe to')
    parser.add_argument('--broker-list', type=str, 
                       default='localhost:9092',
                       help='Kafka broker list (comma-separated)')
    parser.add_argument('--db-path', type=str,
                       default='/data/sensor_data.db',
                       help='Path to SQLite database file')
    
    args = parser.parse_args()
    
    # Environment variables override command-line arguments
    topics = os.getenv('KAFKA_TOPICS', '').split() or args.topics
    broker_list = os.getenv('KAFKA_BROKERS', args.broker_list).split(',')
    db_path = os.getenv('DB_PATH', args.db_path)
    
    subscriber = SensorDataSubscriber(broker_list, topics, db_path)
    subscriber.consume()

if __name__ == "__main__":
    main()