import time
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
from petrel import storm
from petrel.emitter import Spout

log = logging.getLogger("ChicagoCrimeSpout")

class ChicagoCrimeConfluentSpout(Spout):

    def __init__(self):

        log.debug(">>> entered __init___ ...")

        super(ChicagoCrimeConfluentSpout, self).__init__(script=__file__)
        self.conf = None
        self.topic = None
        self.consumer = None
        self.counter = 0
        self.emit_thread = None
        self.message_pool = {}
        self.start_time = 0
        self.end_time = 0

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts
        :param conf: topology.yaml
        :param context:
        :return:
        """
        log.info(">>> entered initialize() ...")

        log.debug(">>> ChicagoCrimeSpout initialize start")
        log.debug(">>> bootstrap.servers: " + conf["ChicagoCrimeSpout.initialize.hosts"])
        log.debug(">>> group.id: " + conf["ChicagoCrimeSpout.initialize.consumer_group"])
        log.debug(">>> session.timeout.ms: " + conf["ChicagoCrimeSpout.initialize.consumer_timeout_ms"])
        log.debug(">>> topic: " + conf["ChicagoCrimeSpout.initialize.topics"])
        kafka_config = {
            'bootstrap.servers': conf["ChicagoCrimeSpout.initialize.hosts"],
            'group.id': bytes(conf["ChicagoCrimeSpout.initialize.consumer_group"], encoding='utf-8'),
            'session.timeout.ms': int(conf["ChicagoCrimeSpout.initialize.consumer_timeout_ms"]),
            'default.topic.config': {'auto.offset.reset': 'largest'}
        }

        self.consumer = Consumer(**kafka_config)

        self.consumer.subscribe([bytes(conf["ChicagoCrimeSpout.initialize.topics"], encoding='utf-8')])

        log.debug(">>> initialize() done ...")

    @classmethod
    def declareOutputFields(cls):
        return ['line']

    def nextTuple(self):
        if self.consumer is None:
            print("self.consumer is not ready yet.")
            return

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info('%% %s [%d] reached end of offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
                storm.emit(msg.value())

def run():
    ChicagoCrimeConfluentSpout().run()