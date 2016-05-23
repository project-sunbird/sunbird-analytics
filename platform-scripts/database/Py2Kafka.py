# Authors: Soma S Dhavala
# demo consuming and producing Kafka topics from python

from kafka import KafkaConsumer
consumer = KafkaConsumer('zerg.hydra')
for msg in consumer:
    print (msg)