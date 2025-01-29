# ===============================================================================
# Copyright 2024 ross
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
import datetime

from sta.definitions import FOOT, OM_Measurement

from stao.base_stao import BQSTAO, LocationMixin, LocationGeoconnexMixin, DatastreamMixin, ObservationMixin
from stao.constants import WATER_WELL, MANUAL_GWL_DS, MANUAL_SENSOR, DTW_OBS_PROP
from stao.util import asiotid

AGENCY = 'BernCo'


class SiteSTAO(BQSTAO):
    _vocab_tag = 'bernco'
    _fields = ['name', 'latitude', 'longitude', 'point_id', 'ose_permit', 'well_depth', 'aquifer_code',
               'casing_stickup', 'screen_interval', 'well_uuid', 'objectid']

    _tablename = 'bernco_arcgis_wells'

    _limit = 500
    _orderby = 'objectid asc'

    def _transform_message(self, record):
        return f"id={record['objectid']}, name={record['name']}"


class BernCoLocations(LocationGeoconnexMixin, LocationMixin, SiteSTAO):

    def _make_location_properties(self, record):
        source_id = self.toST('location.properties.source_id', record)
        properties = {
            'source_id': source_id,
                                         'nmbgmr_id': self.toST('location.properties.nmbgmr_id', record),
                                         'ose_permit': self.toST('location.properties.ose_permit', record),
        }
        return properties

    # def _transform(self, request, record):
    #     payload = self._make_location_payload(record)
    #     source_id = self.toST('location.properties.source_id', record)
    #
    #     payload['properties'] = {'agency': AGENCY,
    #                              'source_id': source_id,
    #                              'nmbgmr_id': self.toST('location.properties.nmbgmr_id', record),
    #                              'ose_permit': self.toST('location.properties.ose_permit', record), }
    #
    #     return payload


class BernCoThings(SiteSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        name = self.toST('location.name', record)

        location = self._client.get_location(f"name eq '{name}'")
        payload = {'name': WATER_WELL['name'],
                   'Locations': [{'@iot.id': location['@iot.id']}],
                   'description': WATER_WELL['description'],
                   'properties': {'agency': AGENCY,
                                  'ose_permit': self.toST('thing.properties.ose_permit', record),
                                    'nmbgmr_id': self.toST('thing.properties.nmbgmr_id', record),
                                    'well_uuid': self.toST('thing.properties.well_uuid', record),
                                    'aquifer_code': self.toST('thing.properties.aquifer_code', record),
                                    'casing_stickup': self.toST('thing.properties.casing_stickup', record),
                                    'screen_interval': self.toST('thing.properties.screen_interval', record),
                                    'well_depth': self.toST('thing.properties.well_depth', record),
                                  }
                   }

        return payload


class BernCoWellDatastreams(DatastreamMixin, SiteSTAO):

    def _transform(self, request, record):
        payload = self._make_datastream_payload(record, 'manual', AGENCY)
        return payload


class BernCoManualGWLObservations(ObservationMixin, BQSTAO):
    _tablename = 'bernco_arcgis_manual_waterlevels as data'
    _fields = ['id', 'well_uuid', 'measurement_date', 'measurement_method', 'depth_to_water_at_measurement_point']
    _limit = 500
    # _where = "or_sensor_id=4"

    # _join = 'nmwdi.ebid_get_sensor_meta_data as s on data.or_sensor_id=s.or_sensor_id'
    _orderby = 'id asc'
    _location_field = 'well_uuid'
    _cursor_id = 'id'
    _datastream_name = MANUAL_GWL_DS['name']
    _thing_name = WATER_WELL['name']
    _agency = AGENCY
    _timestamp_field = 'measurement_date'
    _value_field = 'depth_to_water_at_measurement_point'

    # _check_existing = False
    def _transform_message(self, record):
        return 'Foo'

    def _get_location(self, record, location_id=None, **kw):
        # print(record)
        if location_id is None:
            location_id = record['locationId']
            try:
                location_id = int(location_id)
            except ValueError:
                pass

        q = f"properties/source_id eq '{location_id}' and properties/agency eq '{self._agency}'"
        return self._client.get_location(query=q), location_id

    def _transform_timestamp(self, dt):
        dt = datetime.datetime.fromtimestamp(int(dt/1000))
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    def _transform_value(self, v, record):
        # correct for stickup
        stickup = self._thing['properties'].get('casing_stickup', 0)
        v -= stickup

        return v

    def _extract_timestamp(self, dt):
        return dt

class DummyRequest:
    def __init__(self, p):
        self._p = p

    @property
    def json(self):
        return self._p


if __name__ == '__main__':
    # b = BernCoLocations()
    # b.render(None, dry=False)

    # b = BernCoThings()
    # b.render(None, dry=False)
    c = BernCoManualGWLObservations()
    # b.render(None, dry=True)
    state = None
    for i in range(100):
        print(i, '---------------------------', state)
        if i:
            # state = json.loads(ret)
            dr = DummyRequest(state)
        else:
            dr = DummyRequest({})

        state = c.render(dr, dry=True)
# ============= EOF =============================================
