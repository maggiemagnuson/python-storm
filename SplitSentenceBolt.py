from petrel import storm
from petrel.emitter import BasicBolt

class SplitSentenceBolt(BasicBolt):
    def __init__(self):
        super(SplitSentenceBolt, self).__init__(script=__file__)

    def declareOutputFields(declarer):
        return ['word']

    def process(self, tuple):
        words = tuple.values[0].split(" ")
        for word in words:
            storm.emit([word])

def run():
    SplitSentenceBolt().run()

def test():
    from nose.tools import assert_equal
    from petrel import mock
    bolt = SplitSentenceBolt()
    from RandomSentenceSpout import RandomSentenceSpout
    mock_spout = mock.MockSpout(RandomSentenceSpout.declareOutputFields(), [
                                ["Test this Bolt"]],)

    result = mock.run_simple_topology(None, [mock_spout, bolt], result_type=mock.LIST)
    assert_equal([["Test"], ["this"], ["Bolt"]], result[bolt])
