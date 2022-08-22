from kafka import KafkaConsumer
from json import loads

# used to print out the events
consumer = KafkaConsumer(
    'bank-customer-events',
     bootstrap_servers=['localhost:9092'], # topic
     value_deserializer=lambda m: loads(m.decode('ascii'))) # decode acs-ii

for message in consumer:
    print(message)  # prinout full message contents (log)
    message = message.value  # get value of message
    print('{} found'.format(message))  # log value of message
