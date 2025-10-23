FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY subscriber.py .

# Create data directory
RUN mkdir -p /data

CMD ["python", "subscriber.py", "--topics", "weather", "humidity", "air_quality", "--broker-list", "kafka:9092", "--db-path", "/data/sensor_data.db"]