#!/bin/bash

# Kafka home

kafka_home=/Users/soma/kafka
schema_reg_home=/Users/soma/schema-registry
kafka_rest_home=/Users/soma/kafka-rest

# 4) start kafa-rest proxy
/$kafka_rest_home/bin/kafka-rest-stop

# 3) start schema registery (for Kafka-Rest server to kick in)
/$schema_reg_home/bin/schema-registry-stop

# 2) start kafka server
/$kafka_home/bin/kafka-server-stop.sh

# 1) start zoo-keeper
/$kafka_home/bin/zookeeper-server-stop.sh
