import time
from json import dumps
from kafka import KafkaProducer
import re

# producer - aws
producer = KafkaProducer(bootstrap_servers=['ec2-13-229-46-113.ap-southeast-1.compute.amazonaws.com:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# file = open('quiz1/tf-idf/demo.txt', 'r')
file = open('quiz1/tf-idf/book.txt', 'r')
Lines = file.readlines()
page = 1


def replace_str(string):
    string = re.sub('\W', ' ', string)
    string = string.replace('    ', ' ')
    string = string.replace('   ', ' ')
    string = string.replace('  ', ' ').strip()
    return string


content = ""
for data in Lines:
    if len(re.sub('\W', '', data)) > 1:
        if data.startswith('Page |'):
            sendMsg = str(page) + '|' + content
            page += 1
            sendMsg = sendMsg
            print(sendMsg)
            producer.send('chathai-streams-harry-tf-idf-input', sendMsg)
            content = ""
            # time.sleep(1)
        else:
            content += replace_str(data).encode().decode('utf-8').strip('\n') + " "

producer.flush()
