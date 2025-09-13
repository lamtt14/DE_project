from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json


#Config
keyspace = "lamtt14"
kafka_topic = "newData_from_gen_source"
kafka_bootstrap_servers = "broker:29092"


#Consume data from kafka topic and write to cassandra (table: tracking)
def consume_data_from_kafka(keyspace, kafka_topic, kafka_bootstrap_servers ):
    cluster = Cluster()
    session = cluster.connect(keyspace)
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers= kafka_bootstrap_servers, value_deserializer=lambda v: json.loads(v.decode('utf-8')), group_id = "test-consumer-group", auto_offset_reset = "earliest", )

    for message in consumer:
        data = message.value
        query = """INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ('{}',{},{},'{}',{},{},{},'{}')""".format(data["create_time"], data["bid"], data["campaign_id"], data["custom_track"], data["group_id"], data["job_id"], data["publisher_id"], data["ts"])
        print(query)
        session.execute(query)
        print("Reading data from kafka topic and write to cassandra done!")

    consumer.close()
    cluster.shutdown()


if __name__ == "__main__":
    consume_data_from_kafka(keyspace, kafka_topic, kafka_bootstrap_servers)