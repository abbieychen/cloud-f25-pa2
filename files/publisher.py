#!/usr/bin/env python3
import argparse
import json
import random
import time
import sys
from datetime import datetime
from kafka import KafkaProducer
import logging
import os

# Configuration from environment variables with defaults
BROKER_LIST = os.getenv('BROKER_LIST', 'localhost:9092')
TOPIC = os.getenv('TOPIC', 'weather')
RATE = float(os.getenv('RATE', '1.0'))
TOPIC_PREFIX = os.getenv('TOPIC_PREFIX', 'sensor')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SensorDataPublisher:
    def __init__(self, broker_list, topic_prefix="sensor"):
        self.broker_list = broker_list
        self.topic_prefix = topic_prefix
        self.producer = None
        
    def connect(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.broker_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Connected to Kafka brokers: {self.broker_list}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def generate_weather_data(self, sensor_id):
        """Generate fake weather data"""
        return {
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(-10, 35), 2),  # Celsius
            "humidity": round(random.uniform(30, 90), 2),      # Percentage
            "pressure": round(random.uniform(980, 1020), 2),   # hPa
            "wind_speed": round(random.uniform(0, 50), 2),     # km/h
            "condition": random.choice(["sunny", "cloudy", "rainy", "snowy"]),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def generate_humidity_data(self, sensor_id):
        """Generate fake humidity data"""
        return {
            "sensor_id": sensor_id,
            "humidity": round(random.uniform(20, 95), 2),
            "temperature": round(random.uniform(15, 30), 2),
            "location": random.choice(["indoor", "outdoor"]),
            "unit": "percentage",
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def generate_air_quality_data(self, sensor_id):
        """Generate fake air quality data"""
        return {
            "sensor_id": sensor_id,
            "pm2_5": round(random.uniform(0, 150), 2),        # μg/m³
            "pm10": round(random.uniform(0, 200), 2),         # μg/m³
            "co2": round(random.uniform(300, 1500), 2),       # ppm
            "voc": round(random.uniform(0, 500), 2),          # ppb
            "aqi": random.randint(0, 300),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def generate_power_data(self, sensor_id):
        """Generate fake power consumption data"""
        return {
            "sensor_id": sensor_id,
            "voltage": round(random.uniform(220, 240), 2),    # Volts
            "current": round(random.uniform(0, 15), 2),       # Amps
            "power": round(random.uniform(0, 3500), 2),       # Watts
            "energy": round(random.uniform(0, 100), 2),       # kWh
            "power_factor": round(random.uniform(0.8, 1.0), 3),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def publish_data(self, topic, rate, duration=None):
        """Publish sensor data at specified rate"""
        if not self.producer:
            logger.error("Not connected to Kafka")
            return
        
        sensor_id = f"{topic}_sensor_{random.randint(1000, 9999)}"
        data_generators = {
            "weather": self.generate_weather_data,
            "humidity": self.generate_humidity_data,
            "air-quality": self.generate_air_quality_data,
            "power": self.generate_power_data
        }
        
        generator = data_generators.get(topic, self.generate_weather_data)
        full_topic = f"{self.topic_prefix}.{topic}" if self.topic_prefix else topic
        
        logger.info(f"Starting to publish {topic} data to {full_topic} at {rate} msg/sec")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                if duration and (time.time() - start_time) > duration:
                    break
                
                # Generate and send data
                data = generator(sensor_id)
                self.producer.send(full_topic, value=data)
                message_count += 1
                
                if message_count % 10 == 0:
                    logger.info(f"Published {message_count} messages to {full_topic}")
                
                # Wait to maintain rate
                time.sleep(1.0 / rate)
                
        except KeyboardInterrupt:
            logger.info("Publisher stopped by user")
        except Exception as e:
            logger.error(f"Error during publishing: {e}")
        finally:
            self.producer.flush()
            logger.info(f"Total messages published: {message_count}")

def main():
    parser = argparse.ArgumentParser(description='Kafka Sensor Data Publisher')
    parser.add_argument('--broker-list', default=BROKER_LIST, help='Kafka broker list')
    parser.add_argument('--topic', default=TOPIC, choices=['weather', 'humidity', 'air-quality', 'power'],
                       help='Sensor topic to publish')
    parser.add_argument('--rate', type=float, default=1.0, help='Messages per second')
    parser.add_argument('--duration', type=int, help='Duration to run in seconds (optional)')
    parser.add_argument('--topic-prefix', default='sensor', help='Topic prefix')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create publisher
    publisher = SensorDataPublisher(
        broker_list=args.broker_list.split(','),
        topic_prefix=args.topic_prefix
    )
    
    # Connect to Kafka
    if not publisher.connect():
        sys.exit(1)
    
    # Start publishing
    publisher.publish_data(args.topic, args.rate, args.duration)

if __name__ == "__main__":
    main()