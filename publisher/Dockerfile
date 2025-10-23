FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher.py .

CMD ["python", "publisher.py", "--topic", "weather", "--rate", "1.0", "--broker-list", "kafka:9092"]