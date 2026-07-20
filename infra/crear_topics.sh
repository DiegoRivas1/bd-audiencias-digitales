#!/bin/bash
# Crea los topics necesarios para el proyecto de audiencias digitales.
# Ejecutar desde ~/kafka con el broker ya corriendo.
set -e

BROKER="localhost:9092"

bin/kafka-topics.sh --create --if-not-exists --topic user-events \
    --bootstrap-server "$BROKER" --partitions 6 --replication-factor 1

bin/kafka-topics.sh --create --if-not-exists --topic purchase-events \
    --bootstrap-server "$BROKER" --partitions 6 --replication-factor 1

echo "== Topics creados =="
bin/kafka-topics.sh --bootstrap-server "$BROKER" --list
