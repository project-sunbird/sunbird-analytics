#!/bin/bash

# Kafka home
kafka_home=/Users/soma/kafka
schema_reg_home=/Users/soma/schema-registry
kafka_rest_home=/Users/soma/kafka-rest

# 1) start zoo-keeper
$kafka_home/bin/zookeeper-server-start.sh $kafka_home/config/zookeeper.properties &
# 2) start kafka server
$kafka_home/bin/kafka-server-start.sh $kafka_home/config/server.properties &
# 3) start schema registery (for Kafka-Rest server to kick in)
$schema_reg_home/bin/schema-registry-start $schema_reg_home/config/schema-registry.properties &
# 4) start kafa-rest proxy
$kafka_rest_home/bin/kafka-rest-start $kafka_rest_home/config/kafka-rest.properties &