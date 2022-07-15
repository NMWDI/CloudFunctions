# ===============================================================================
# Copyright 2022 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
from itertools import groupby

from sta.definitions import FOOT, OM_Measurement

try:
    from constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, \
        ThingMixin, DatastreamMixin, SimpleSTAO
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid, make_statime
except ImportError:
    from stao.constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, \
        ThingMixin, DatastreamMixin, SimpleSTAO
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid, make_statime

AGENCY = 'PVACD'


class PHV_Site_STAO(BQSTAO):
    _vocab_tag = 'phv'
    _fields = ['id', 'name', 'latitude', 'longitude', 'description']

    _dataset = 'locations'
    _tablename = 'pecos_locations'

    _limit = 100
    _orderby = 'id asc'
    _where = "LOWER(name) like '%level%'"

    def _transform_message(self, record):
        return f"id={record['id']}, name={record['name']}"


class PHVLocations(LocationGeoconnexMixin, PHV_Site_STAO, LocationMixin):
    _entity_tag = 'location'

    def _transform(self, request, record):
        payload = self._make_location_payload(record)

        source_id = self.toST('location.properties.source_id', record)
        hvd = self.toST('location.properties.hydrovu_description', record)

        payload['properties'] = {'agency': AGENCY,
                                 'source_id': source_id,
                                 'hydrovu.description': hvd}
        return payload


class PHVThings(PHV_Site_STAO, ThingMixin):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        payload = self._make_thing_payload(record)
        payload['properties'] = {'agency': AGENCY,
                                 'source_id': self.toST('thing.properties.source_id', record)}

        return payload


class PHVWaterLevelsDatastreams(PHV_Site_STAO, DatastreamMixin):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        payload = self._make_datastream_payload(record, 'gwl', AGENCY)
        payload['properties'] = {}
        return payload


class PHVObservations(ObservationMixin, BQSTAO):
    _vocab_tag = 'phv'

    _tablename = 'pecos_readings'
    _fields = ['value', 'unitId', 'timestamp',
               'locationId', 'parameterId', 'customParameter', '_airbyte_ab_id']
    _limit = 500
    _where = "parameterId=4"

    _dataset = 'levels'
    _entity_tag = 'observation'

    # _orderby = 'timestamp asc'
    _orderby = '_airbyte_ab_id asc'
    _location_field = 'locationId'
    _cursor_id = '_airbyte_ab_id'
    _datastream_name = GWL_DS['name']
    _thing_name = WATER_WELL['name']
    _agency = AGENCY
    _timestamp_field = 'timestamp'
    _value_field = 'value'

    def _extract_timestamp(self, dt):
        return dt

    def _transform_value(self, v):
        """convert m to ft"""
        return v * 3.281


if __name__ == '__main__':
    # phv = PHVLocations()
    # phv = PHVThings()

    # phv = PHVWaterLevelsDatastreams()
    # ss = SimpleSTAO()
    # ss.render('sensor', HYDROVU_SENSOR)
    # class DummyRequest:
    #     def __init__(self, p):
    #         self._p = p
    #
    #     @property
    #     def json(self):
    #         return self._p
    #
    # phv = PHVObservations()
    # phv.render(None, dry=False)
    # state = None
    # for i in range(2):
    #     print(i, '---------------------------', state)
    #     if i:
    #         # state = json.loads(ret)
    #         dr = DummyRequest(state)
    #     else:
    #         dr = DummyRequest({})
    #
    #     state = phv.render(dr)
    # phv = PHVLocations()
    #
    # phv = PHVThings()
    # phv.render(None, dry=False)
    #
    # phv = PHVWaterLevelsDatastreams()
    # phv.render(None, dry=False)
    #
    phv = PHVObservations()
    phv.render(None, dry=False)
# ============= EOF =============================================
