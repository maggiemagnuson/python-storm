from collections import defaultdict
from petrel import storm
from petrel.emitter import BasicBolt

class WordCountBolt(BasicBolt):
    def __init__(self):
        super(WordCountBolt, self).__init__(script=__file__)
        self._count = defaultdict(int)

    def declareOutputFields(declarer):
        return(['word', 'count'])

    def process(self, tuple):
        word = tuple.values[0]
        self._count[word] += 1
        storm.emit([word, self._count[word]])

def run():
    WordCountBolt().run()

def testWordCountBolt():
    from nose.tools import assert_equal

    bolt = WordCountBolt()

    from petrel import mock
    from RandomSentenceSpout import RandomSentenceSpout
    mock_spout = mock.MockSpout(RandomSentenceSpout.declareOutputFields(), [
        ['word'],
        ['other'],
        ['word']
    ])

    result = mock.run_simple_topology(None, [mock_spout, bolt], result_type=mock.LIST)
    assert_equal(2, bolt._count['word'])
    assert_equal(1, bolt._count['other'])
    assert_equal([['word', 1], ['other', 1], ['word', 2]], result[bolt])
