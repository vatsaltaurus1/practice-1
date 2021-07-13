from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     # value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for message in consumer:
    message = message.value
    print(message)
    # collection.insert_one(message)
    # print('{} added to {}'.format(message, collection))

# zkServer start-foreground
# kafka-server-start /usr/local/etc/kafka/server.properties
# kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
# kafka-console-producer --broker-list localhost:9092 --topic test