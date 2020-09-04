from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_END
# from confluent_kafka.avro import AvroConsumer
# from confluent_kafka.avro.serializer import SerializerError
import json, pandas


class kafkawrapper(object):
    def __init__(self, bootstrap_server):
        self._bootstrap_server = bootstrap_server
    def _connect(self,topic):
        self._topic = topic
        self._consumer = Consumer({
            "bootstrap.servers": self._bootstrap_server,
            "group.id":"some_test_group",
            "enable.auto.commit": False,
            "on_commit": self._log_on_commit,
            "auto.offset.reset":"beginning"
        })
    def eq(self, nor):
        self._consumer.subscribe([self._topic])
        messages = []
        while self._status:
            record = None
            records = self._consumer.consume(nor,10.0)
            if len(records) < nor:
                self._status = False
            for r in records:
                record = self.process_message(r)
                messages.append(record)
        return messages
    def process_message(self, m=None):
        if m is None:
            self._status = False
            return None
        record = [m.offset(), m.key(), m.value().decode("utf-8")]
        return record


bs = "localhost:9092"
topic = "test-topic"
nor = 5


kw = kafkawrapper(bs)
kw._connect(topic)
kw.eq(nor)