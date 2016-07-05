#!/bin/bash

# Kafka home
/Users/soma/kafka

# 1) start zoo-keeper
./bin/zookeeper-server-start.sh ./kafka/config/zookeeper.properties &
# 2) Start Kafka server
./bin/kafka-server-start.sh ./config/server.properties &
# 3) Start schema registery (for Kafka-Rest server to kick in)
$ ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties &
# 4) Start kafa-rest proxy
$ ./bin/kafka-rest-start ./etc/kafka-rest/kafka-rest.properties &