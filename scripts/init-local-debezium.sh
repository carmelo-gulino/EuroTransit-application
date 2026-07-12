#!/bin/bash

# ==============================================================================
# EuroTransit - Local Debezium Initialization Script
# ==============================================================================
# This script registers the PostgreSQL Debezium connector in the local Kafka Connect
# instance running via docker-compose. It enables the Outbox Pattern for the
# Inventory service.
#
# Requirements: 
# - docker compose up -d (postgres, kafka, and debezium must be running)
# ==============================================================================

echo "Waiting for Debezium to be ready on localhost:8083..."
while ! curl -s http://localhost:8083/ > /dev/null; do
  sleep 2
  echo -n "."
done
echo -e "\nDebezium is up!"

echo "Registering 'inventory-outbox-connector'..."

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors -d '{
  "name": "inventory-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "inventory",
    "database.password": "password",
    "database.dbname": "inventory_db",
    "topic.prefix": "inventory",
    "plugin.name": "pgoutput",
    "table.include.list": "public.outbox_events",
    
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.route.by.field": "type"
  }
}'

echo -e "\n\nConnector registered successfully! Debezium is now listening to the outbox_events table."
