from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random


# generates *reasonable* random data
class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):  # service routine
        data = {'custid' : random.randint(50,56),  # range of ids is 50-55
            'type': self.depOrWth(),  # type is deposit or withdraw
            'date': int(time.time()),  #
            'amt': random.randint(10,101)*100,
            }
        return data

    def depOrWth(self):  # service routine
        return 'dep' if (random.randint(0,2) == 0) else 'wth' # coin flip for deposit or withdraw

    def generateRandomXactions(self, n=1000):  # generates random transactions
        for _ in range(n):
            data = self.emit()  # create a dictionary of random transaction
            print('sent', data)  # printing it out
            self.producer.send('bank-customer-events', value=data)  # sending to customer events
            sleep(1)

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)
