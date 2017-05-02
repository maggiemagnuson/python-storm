import RandomSentenceSpout
import SplitSentenceBolt
import WordCountBolt
import ChicagoCrimeSpout
import ChicagoCrimeConfluentSpout
import ChicagoCrimeSplitterBolt

def create(builder):
    builder.setSpout("spout", ChicagoCrimeSpout.ChicagoCrimeSpout(), 1)
    builder.setBolt("bolt", ChicagoCrimeSplitterBolt.ChicagoCrimeSplitterBolt(), 1).shuffleGrouping("spout")
    #builder.setSpout("spout", ChicagoCrimeConfluentSpout.ChicagoCrimeConfluentSpout(), 1)
    #builder.setSpout("spout", RandomSentenceSpout.RandomSentenceSpout(), 1)
    #builder.setBolt("split", SplitSentenceBolt.SplitSentenceBolt(), 1).shuffleGrouping("spout")
    #builder.setBolt("count", WordCountBolt.WordCountBolt(), 1).fieldsGrouping("split", ["word"])