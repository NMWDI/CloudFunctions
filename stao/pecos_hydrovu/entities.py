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
    from constants import WATER_WELL
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
        LOCATION_DESCRIPTION, asiotid, make_statime
except ImportError:
    from stao.constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid, make_statime

AGENCY = 'PVACD'


def clean_name(name):
    """
    remove level/Level from the name
    :param name:
    :return:
    """
    return name.replace('level', '').replace('Level', '')


class PHV_Site_STAO(BQSTAO):
    _fields = ['id', 'name', 'latitude', 'longitude', 'description']

    _dataset = 'locations'
    _tablename = 'pecos_locations'

    _limit = 100
    _orderby = 'id asc'
    _where = "LOWER(name) like '%level%'"

    def _transform_message(self, record):
        return f"id={record['id']}, name={record['name']}"


class PHVLocations(LocationGeoconnexMixin, PHV_Site_STAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        properties = {'agency': AGENCY,
                      'source_id': record['id'],
                      'hydrovu.description': record['description']}

        lat = record['latitude']
        lon = record['longitude']

        payload = {'name': clean_name(record['name']),
                   'description': 'Location of well where measurements are made',
                   'properties': properties,
                   'location': make_geometry_point_from_latlon(lat, lon),
                   "encodingType": "application/vnd.geo+json",
                   }

        return payload


class PHVThings(PHV_Site_STAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        name = clean_name(record['name'])
        location = self._client.get_location(f"name eq '{name}'")

        payload = {'name': WATER_WELL['name'],
                   'Locations': [{'@iot.id': location['@iot.id']}],
                   'description': WATER_WELL['description'],
                   'properties': {'agency': AGENCY,
                                  'source_id': record['id']}
                   }

        return payload


class PHVWaterLevelsDatastreams(PHV_Site_STAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        name = clean_name(record['name'])
        q = f"name eq '{name}' and properties/agency eq '{AGENCY}'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {name}')
            return

        thing = self._client.get_thing(location=loc['@iot.id'], name=WATER_WELL['name'])
        if thing:
            obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
            sensor = next(self._client.get_sensors(name=HYDROVU_SENSOR['name']))
            properties = {}
            dtwbgs = {'name': GWL_DS['name'],
                      'description': GWL_DS['description'],
                      'Sensor': asiotid(sensor),
                      'ObservedProperty': asiotid(obsprop),
                      'Thing': asiotid(thing),
                      'unitOfMeasurement': FOOT,
                      'observationType': OM_Measurement,
                      'properties': properties
                      }
            return dtwbgs


class PHVObservations(BQSTAO, ObservationMixin):
    _tablename = 'pecos_readings'
    _fields = ['value', 'unitId', 'timestamp',
               'locationId', 'parameterId', 'customParameter', '_airbyte_ab_id']
    _limit = 500
    _where = "parameterId=4"

    _dataset = 'levels'
    _entity_tag = 'observation'

    _orderby = 'timestamp asc'
    _location_field = 'locationId'
    _cursor_id = '_airbyte_ab_id'
    _datastream_name = GWL_DS['name']
    _thing_name = WATER_WELL['name']
    _agency = AGENCY
    _timestamp_field = 'timestamp'
    _value_field = 'value'



if __name__ == '__main__':
    phv = PHVLocations()
    phv.render(None, dry=True)

# ============= EOF =============================================
