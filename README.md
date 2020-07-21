# asterisk-res_kafka
Kafka resources for Asterisk

This module provide backend to connect Kafka brokers as producer and consumer services.

Configuration stored by Asterisk sorcery, default in file kafka.conf

[cluster_1]

type=cluster

brokers=rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094

security_protocol=sasl_ssl

sasl_mechanism=SCRAM-SHA-256

sasl_username=...

sasl_password=...


[producer_a]

type=producer
cluster=cluster_1

[consumer_b]

type=consumer
cluster=cluster_1
partition=0

[topic_a]

type=topic
pipe=pipe_1
topic=topic_for_producer
producer=producer_a

[topic_b]
type=topic
pipe=pipe_1
topic=topic_for_consumer
consumer=consumer_a

For this example if other Asterisk modules send a message to the "pipe_1",
than message posted to the topic "topic_for_producer" at cluster "cluster_1".
If other Asterisk modules need to subscribe topic "topic_for_consumer" from "cluster_1"
it must read "pipe_1".
