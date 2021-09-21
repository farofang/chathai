import time
from json import dumps
from kafka import KafkaProducer

# producer - aws
producer = KafkaProducer(bootstrap_servers=['ec2-13-229-46-113.ap-southeast-1.compute.amazonaws.com:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# file = open('quiz1/wordcount/book_demo.txt', 'r')
file = open('quiz1/wordcount/book.txt', 'r')
Lines = file.readlines()
i = 1
lines_count = len(Lines)
print(len(Lines))
for data in Lines:
    print('Line no: {} from {}'.format(i, lines_count))
    i += 1
    sendMsg = data.encode().decode('utf-8').strip('\n')
    producer.send('chathai-streams-harrycount-input', sendMsg)
    time.sleep(1)

producer.flush()
