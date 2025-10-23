#!/bin/bash
REGISTRY="192.168.1.100:30000"

# Build and push publisher
cd publisher
docker build -t $REGISTRY/sensor-publisher:latest .
docker push $REGISTRY/sensor-publisher:latest
cd ..

# Build and push subscriber
cd subscriber
docker build -t $REGISTRY/sensor-subscriber:latest .
docker push $REGISTRY/sensor-subscriber:latest
cd ..

# Build and push webserver
cd webserver
docker build -t $REGISTRY/sensor-webserver:latest .
docker push $REGISTRY/sensor-webserver:latest
cd ..