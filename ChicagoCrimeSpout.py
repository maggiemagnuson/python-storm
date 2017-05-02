import time
import logging
from datetime import datetime
from pykafka import KafkaClient
from petrel import storm
from petrel.emitter import Spout

log = logging.getLogger("ChicagoCrimeSpout")

class ChicagoCrimeSpout(Spout):

    def __init__(self):

        log.debug(">>> entered __init___ ...")

        super(ChicagoCrimeSpout, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.topic = None
        self.consumer = None
        self.counter = 0
        self.emit_thread = None

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts
        :param conf: topology.yaml
        :param context:
        :return:
        """
        log.info(">>> ChicagoCrimeSpout() initialize start ...")

        self.conf = conf
        self.client = KafkaClient(hosts=conf["ChicagoCrimeSpout.initialize.hosts"])
        self.topic = self.client.topics[bytes(conf["ChicagoCrimeSpout.initialize.topics"],encoding='utf-8')]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=bytes(conf["ChicagoCrimeSpout.initialize.consumer_group"], encoding='utf-8'),
            zookeeper_connect=str(conf["ChicagoCrimeSpout.initialize.zookeeper"]),
            consumer_timeout_ms=int(conf["ChicagoCrimeSpout.initialize.consumer_timeout_ms"]),
            auto_commit_enable=True
        )

        log.debug(" >>> ChicagoCrimeSpout() initialize done ...")
        log.debug(">>> ChicagoCrimeSpout initialize start")
        log.debug(">>> bootstrap.servers: " + conf["ChicagoCrimeSpout.initialize.hosts"])
        log.debug(">>> group.id: " + conf["ChicagoCrimeSpout.initialize.consumer_group"])
        log.debug(">>> session.timeout.ms: " + conf["ChicagoCrimeSpout.initialize.consumer_timeout_ms"])

    @classmethod
    def declareOutputFields(cls):
        return ['line']

    def nextTuple(self):
        if self.consumer is None:
            print("self.consumer is not ready yet.")
            return

        try:
            for message in self.consumer:
               if message is not None:
                    msg_id = str(self.counter)
                    #log.info(">>> MESSAGE: " + message.value.decode('ascii'))
                    storm.emit([message.value.decode('ascii')])
                    self.counter += 1
                    #log.info(">>>> COUNTER: " + self.counter)

        except Exception as inst:
            log.debug("Exception Type: %s ; Args: %s", type(inst), inst.args)

def run():
    ChicagoCrimeSpout().run()