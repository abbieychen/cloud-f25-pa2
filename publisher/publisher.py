#!/usr/bin/env python3
import json
import time
import random
import argparse
import os
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensorDataPublisher:
    def __init__(self, broker_list, topic, rate):
        self.broker_list = broker_list
        self.topic = topic
        self.rate = rate  # messages per second
        self.producer = None
        
    def connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.broker_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            logger.info(f"Connected to Kafka brokers: {self.broker_list}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def generate_sensor_data(self, topic):
        base_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'sensor_id': f"sensor_{random.randint(1000, 9999)}"
        }
        
        if topic == 'weather':
            data = {
                **base_data,
                'temperature': round(random.uniform(-10, 40), 2),
                'humidity': round(random.uniform(0, 100), 2),
                'pressure': round(random.uniform(980, 1050), 2),
                'wind_speed': round(random.uniform(0, 100), 2)
            }
        elif topic == 'humidity':
            data = {
                **base_data,
                'humidity': round(random.uniform(0, 100), 2),
                'dew_point': round(random.uniform(-10, 30), 2),
                'absolute_humidity': round(random.uniform(0, 30), 2)
            }
        elif topic == 'air_quality':
            data = {
                **base_data,
                'pm2_5': round(random.uniform(0, 300), 2),
                'pm10': round(random.uniform(0, 500), 2),
                'co2': round(random.uniform(300, 2000), 2),
                'voc': round(random.uniform(0, 500), 2)
            }
        else:  # generic sensor
            data = {
                **base_data,
                'value': round(random.uniform(0, 100), 2),
                'unit': 'units'
            }
        
        return data
    
    def publish(self):
        if not self.connect():
            return
        
        interval = 1.0 / self.rate
        logger.info(f"Starting to publish {self.rate} messages/sec to topic '{self.topic}'")
        
        try:
            while True:
                data = self.generate_sensor_data(self.topic)
                future = self.producer.send(self.topic, data)
                
                # Optional: wait for acknowledgment
                future.get(timeout=10)
                
                logger.info(f"Published to {self.topic}: {data}")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping publisher...")
        except Exception as e:
            logger.error(f"Error during publishing: {e}")
        finally:
            if self.producer:
                self.producer.close()

def main():
    parser = argparse.ArgumentParser(description='Sensor Data Publisher')
    parser.add_argument('--topic', type=str, required=True,
                       help='Kafka topic to publish to')
    parser.add_argument('--rate', type=float, default=1.0,
                       help='Publishing rate in messages per second')
    parser.add_argument('--broker-list', type=str, 
                       default='localhost:9092',
                       help='Kafka broker list (comma-separated)')
    
    args = parser.parse_args()
    
    # Environment variables override command-line arguments
    topic = os.getenv('KAFKA_TOPIC', args.topic)
    rate = float(os.getenv('PUBLISH_RATE', args.rate))
    broker_list = os.getenv('KAFKA_BROKERS', args.broker_list).split(',')
    
    publisher = SensorDataPublisher(broker_list, topic, rate)
    publisher.publish()

if __name__ == "__main__":
    main()