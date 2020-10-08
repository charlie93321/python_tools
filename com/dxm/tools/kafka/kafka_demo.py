import json

from kafka import KafkaProducer
from pykafka import KafkaClient

bootstrap_servers = '124.70.208.68:9090,124.70.208.68:9091,124.70.208.68:9092'




def get_all_topics():
    client = KafkaClient(hosts=bootstrap_servers)
    for topic in client.topics:
        print("TOPIC==>>{}".format(topic))



def get_all_brokes():
    from pykafka import KafkaClient
    client = KafkaClient(hosts=bootstrap_servers)
    print(client.brokers)

    for n in client.brokers:
        host = client.brokers[n].host
        port = client.brokers[n].port
        id = client.brokers[n].id
        print("host=%s | port=%s | broker.id=%s " % (host, port, id))


def put_msg_in_kafka(key:str,data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic="WordCount",key=key.encode("utf-8"),value=json.dumps(data,ensure_ascii=False).encode("utf-8"))



#put_msg_in_kafka(key="test1",data={'id':1001,'value':'tom'})

from kafka import KafkaConsumer

consumer = KafkaConsumer('WordCount', bootstrap_servers=bootstrap_servers.split(","))
for msg in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print(recv)
