import time
import random
import logging
from petrel import storm
from petrel.emitter import Spout

log = logging.getLogger("RandomSentenceSpout")

log.debug("RandomSentenceSpout loading")

class RandomSentenceSpout(Spout):

    def __init__(self):
        super(RandomSentenceSpout, self).__init__(script=__file__)

    @classmethod
    def declareOutputFields(cls):
        return ['sentence']

    sentences = [
        "the cow jumped over the moon",
        "an apple a day keeps the docter away",
        "four score and seven years ago",
        "snow white and the seven dwarfs",
        "i am at two with nature"
    ]

    def nextTuple(self):
        time.sleep(0.25)
        sentence = self.sentences[random.randint(0, len(self.sentences)-1)]

        log.debug("RandomSentence nextTuple emitting %s", sentence)
        storm.emit([sentence])

def test():
    from nose.tools import assert_true
    from petrel import mock

    spout = RandomSentenceSpout()

    result = mock.run_simple_topology(None, [spout])
    assert_true(isinstance(result[spout][0], str))

def run():
    # This is a good place to do additional initialization.
    RandomSentenceSpout().run()