# ===============================================================================
# Copyright 2021 ross
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
from sta.definitions import FOOT, OM_Measurement

try:
    from stao import BQSTAO, LocationGeoconnexMixin
    from util import make_geometry_point_from_latlon, asiotid
    from constants import DTW_OBS_PROP, WATER_WELL, GWL_DS
except ImportError:
    from stao.stao import BQSTAO, LocationGeoconnexMixin
    from stao.util import make_geometry_point_from_latlon, asiotid
    from stao.constants import DTW_OBS_PROP, WATER_WELL, GWL_DS

AGENCY = 'ISC_SEVEN_RIVERS'


class ISCSevenRiversMonitoringPoints(BQSTAO):
    _fields = ['id', 'name', 'type', 'comments', 'latitude', 'longitude', 'groundSurfaceElevationFeet']
    _dataset = 'locations'
    _tablename = 'isc_seven_rivers_monitoring_points'


class ISCSevenRiversLocationsSTAO(ISCSevenRiversMonitoringPoints, LocationGeoconnexMixin):
    _entity_tag = 'location'

    def _transform(self, request, record):
        """
        return a ST compitable object
        :param request:
        :param records:
        :return:
        """

        loc = make_geometry_point_from_latlon(record['latitude'], record['longitude'])
        props = {'source_id': record['id'],
                 'agency': AGENCY,
                 'source_api': 'https://nmisc-wf.gladata.com/api/getMonitoringPoints.ashx',
                 'groundSurfaceElevationFeet': record['groundSurfaceElevationFeet']}
        obj = {'name': record['name'],
               'description': record['comments'] or 'No Description',
               'location': loc,
               'properties': props,
               "encodingType": "application/vnd.geo+json", }

        return obj


class ISCSevenRiversThingsSTAO(ISCSevenRiversMonitoringPoints):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        name = record['name']
        location = self._client.get_location(f"name eq '{name}'")
        props = {'type': record['type']}
        obj = {'name': WATER_WELL,
               'description': 'No Description',
               'properties': props,
               'Locations': [{'@iot.id': location['@iot.id']}]}
        return obj


class ISCSevenRiversSensors(ISCSevenRiversMonitoringPoints):
    _entity_tag = 'sensor'

    def _transform(self, request, record):
        payload = {'name': 'NoSensor',
                   'description': 'No Description',
                   'encodingType': 'application/pdf',
                   'metadata': 'No Metadata'}

        return payload


class ISCSevenRiversObservedProperties(ISCSevenRiversMonitoringPoints):
    _entity_tag = 'observed_property'

    def _transform(self, request, record):
        payload = DTW_OBS_PROP
        return payload


class ISCSevenRiversDatastreams(ISCSevenRiversMonitoringPoints):
    _entity_tag = 'datastream'

    def _transform(self, request, record):

        loc = self._client.get_location(f"name eq '{record['name']}' and properties/agency eq '{AGENCY}'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL, location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))

                sensor = next(self._client.get_sensors(name='NoSensor'))
                thing_id = asiotid(thing)
                obsprop_id = asiotid(obsprop)
                sensor_id = asiotid(sensor)
                properties = {}
                payload = { 'name': GWL_DS['name'],
                            'description': GWL_DS['description'],
                            'Thing': thing_id,
                            'ObservedProperty': obsprop_id,
                            'Sensor': sensor_id,
                            'unitOfMeasurement': FOOT,
                            'observationType': OM_Measurement,
                            'properties': properties
                            }
                return payload
            else:
                print(f'no thing (WaterWell) for {lid}')
        else:
            print(f"no location for {record['name']}")


def etl_locations(request):
    stao = ISCSevenRiversLocationsSTAO()
    return stao.render(request)


def etl_things(request):
    stao = ISCSevenRiversThingsSTAO()
    return stao.render(request)


if __name__ == '__main__':
    etl_things(None)
# ============= EOF =============================================
