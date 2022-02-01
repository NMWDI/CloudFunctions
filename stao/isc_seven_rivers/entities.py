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
    from util import make_geometry_point_from_latlon
except ImportError:
    from stao.stao import BQSTAO, LocationGeoconnexMixin
    from stao.util import make_geometry_point_from_latlon, asiotid


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
                 'agency': 'ISC_SEVEN_RIVERS',
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
        obj = {'name': 'Water Well',
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
        payload = {'name': 'Depth to Water Below Ground Surface',
                   'description': 'depth to water below ground surface',
                   'definition': 'No Definition'}
        return payload


class ISCSevenRiversDatastreams(ISCSevenRiversMonitoringPoints):
    _entity_tag = 'datastream'

    def _transform(self, request, record):

        loc = self._client.get_location(f"name eq {record['name']}&$filter=properties/agency='ISC_SEVEN_RIVERS'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name='WaterWell', location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name='Depth to Water Below Ground Surface'))

                sensor = next(self._client.get_sensors(name='NoSensor'))
                thing_id = asiotid(thing['@iot.id'])
                obsprop_id = asiotid(obsprop['@iot.id'])
                sensor_id = asiotid(sensor['@iot.id'])
                properties = {}
                payload = [{'name': 'Groundwater Levels',
                            'description': 'Measurement of groundwater depth in a water well, as measured below ground surface',
                            'Thing': thing_id,
                            'ObservedProperty': obsprop_id,
                            'Sensor': sensor_id,
                            'unitOfMeasurement': FOOT,
                            'observationType': OM_Measurement,
                            'properties': properties
                            }]
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
