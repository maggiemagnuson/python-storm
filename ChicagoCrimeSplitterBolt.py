from collections import defaultdict
from petrel import storm
from petrel.emitter import BasicBolt
import logging
import json
import re

log = logging.getLogger("ChicagoCrimeSplitterBolt")

class ChicagoCrimeSplitterBolt(BasicBolt):
    def __init__(self):
        super(ChicagoCrimeSplitterBolt, self).__init__(script=__file__)
        self._count = defaultdict(int)

    def declareOutputFields(declarer):
        return ["obj"]

    def process(self, tuple):
        val = tuple.values[0]
        line = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").split(val)

        cct = ChicagoCrimeObject()
        ccl = ChicagoCrimeLocation()
        ccb = ChicagoCrimeBeat()

        cct.id = str(line[0])
        cct.case_number = str(line[1])
        cct.date = str(line[2])
        cct.block = str(line[3])
        cct.iucr = str(line[4])
        cct.primary_type = str(line[5])
        cct.description = str(line[6])
        ccl.location_description = str(line[7])
        ccl.location = str(line[21])
        ccl.longitude = str(line[19])
        ccl.latitude = str(line[20])
        ccl.x_coordinate = str(line[15])
        ccl.y_coordinate = str(line[16])
        cct.location = ccl.toJSON()
        cct.arrest = str(line[8])
        cct.domestic = str(line[9])
        ccb.beat = str(line[10])
        ccb.community_area = str(line[13])
        ccb.district = str(line[11])
        ccb.ward = str(line[12])
        cct.beat = ccb.toJSON()
        cct.fbi_code = str(line[14])
        cct.year = str(line[17])
        cct.updated_on = str(line[18])

        log.info(cct.toJSON())
        storm.emit([cct.toJSON()])
        #storm.emit([word, self._count[word]])

def run():
    ChicagoCrimeSplitterBolt().run()


def testWordCountBolt():
    from nose.tools import assert_equal

    bolt = ChicagoCrimeSplitterBolt()

    from petrel import mock
    from ChicagoCrimeSpout import ChicagoCrimeSpout
    mock_spout = mock.MockSpout(ChicagoCrimeSpout.declareOutputFields(), [
        ["2147528,HH392600,05\/23\/2002 11:30:00 PM,011XX N LAWNDALE AVE,0925,MOTOR VEHICLE THEFT,\"ATT: TRUCK, BUS, MOTOR HOME\",STREET,false,false,1112,011,27,23,07,1151504,1907267,2002,04\/15\/2016 08:55:02 AM,41.901422057,-87.718953928,\"(41.901422057, -87.718953928)\""]
    ])

    result = mock.run_simple_topology(None, [mock_spout, bolt], result_type=mock.LIST)

    assert_equal(" MOTOR HOME")

class ChicagoCrimeBeat(object):

    def __init__(self):
        self.beat = None
        self.district = None
        self.ward = None
        self.community_area = None

    def toJSON(self):
        return json.dumps(self.__dict__, sort_keys=False, indent=4)

class ChicagoCrimeObject(object):

    def __init__(self):
        self.id = None
        self.case_number = None
        self.date = None
        self.block = None
        self.iucr = None
        self.primary_type = None
        self.description = None
        self.arrest = None
        self.domestic = None
        self.beat = None
        self.fbi_code = None
        self.location = None
        self.year = None
        self.updated_on = None

    def toJSON(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=4)

class ChicagoCrimeLocation:

    def __init__(self):
        self.location_description = None
        self.x_coordinate = None
        self.y_coordinate = None
        self.latitude = None
        self.longitude = None
        self.location = None

    def toJSON(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=4)