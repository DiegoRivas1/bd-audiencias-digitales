#!/bin/bash
# Instala Java + Apache Kafka (modo KRaft) en una EC2 Ubuntu.
# Uso: bash setup_kafka.sh <IP_PUBLICA_EC2>
set -e

IP_PUBLICA="$1"
if [ -z "$IP_PUBLICA" ]; then
    echo "Uso: bash setup_kafka.sh <IP_PUBLICA_EC2>"
    exit 1
fi

echo "== Instalando Java =="
sudo apt update
sudo apt install -y openjdk-17-jdk

echo "== Descargando Apache Kafka =="
cd ~
if [ ! -d kafka ]; then
    wget -q https://downloads.apache.org/kafka/4.2.1/kafka_2.13-4.2.1.tgz
    tar -xzf kafka_2.13-4.2.1.tgz
    mv kafka_2.13-4.2.1 kafka
fi
cd kafka

echo "== Formateando almacenamiento (KRaft) =="
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties

echo "== Configurando listeners con IP publica: $IP_PUBLICA =="
sed -i '/^listeners=/d; /^advertised.listeners=/d' config/server.properties
{
    echo "listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093"
    echo "advertised.listeners=PLAINTEXT://${IP_PUBLICA}:9092"
} >> config/server.properties

echo "== Listo. Para levantar el broker: =="
echo "cd ~/kafka && bin/kafka-server-start.sh config/server.properties"
