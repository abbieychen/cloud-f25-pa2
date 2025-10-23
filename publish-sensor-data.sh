#!/bin/bash
TIMESTAMP=$(date +%Y%m%d%H%M%S)

# Launch weather publisher
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: sensor-publisher-weather-${TIMESTAMP}
spec:
  template:
    spec:
      containers:
      - name: publisher
        image: 192.168.1.100:30000/sensor-publisher:latest
        command: ["python", "publisher.py"]
        args:
          - "--topic"
          - "weather"
          - "--rate"
          - "2.0"
          - "--broker-list"
          - "kafka-service:9092"
      restartPolicy: Never
  backoffLimit: 3
EOF

# Launch humidity publisher  
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: sensor-publisher-humidity-${TIMESTAMP}
spec:
  template:
    spec:
      containers:
      - name: publisher
        image: 192.168.1.100:30000/sensor-publisher:latest
        command: ["python", "publisher.py"]
        args:
          - "--topic"
          - "humidity"
          - "--rate"
          - "1.5"
          - "--broker-list"
          - "kafka-service:9092"
      restartPolicy: Never
  backoffLimit: 3
EOF