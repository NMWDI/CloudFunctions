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
import datetime
from itertools import groupby

from sta.definitions import FOOT, OM_Measurement

try:
    from stao import BQSTAO, LocationGeoconnexMixin
    from util import make_geometry_point_from_latlon, asiotid
    from constants import DTW_OBS_PROP, WATER_WELL, GWL_DS
except ImportError:
    from stao.stao import BQSTAO, LocationGeoconnexMixin, ObservationMixin
    from stao.util import make_geometry_point_from_latlon, asiotid, make_statime
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


class ISCSevenRiversWaterLevels(BQSTAO, ObservationMixin):
    _tablename = 'isc_water_levels'
    _fields = ['dry', 'invalid', 'comments',
               'monitoring_point_id', 'dateTime', 'depthToWaterFeet']
    _limit = 500

    _dataset = 'levels'
    _entity_tag = 'observation'

    _orderby = '_airbyte_emitted_at asc'
    _timestamp_field = 'dateTime'
    _value_field = 'depthToWaterFeet'
    _cursor_id = '_airbyte_emitted_at'

    def _handle_extract(self, records):
        def key(r):
            return int(r['monitoring_point_id'])

        maxo = None
        for g, obs in groupby(sorted(records, key=key), key=key):
            obs = list(obs)
            t = max((o[self._cursor_id] for o in obs))
            if maxo:
                maxo = max(maxo, t)
            else:
                maxo = t
            yield {'monitoring_point_id': g, 'observations': obs, self._cursor_id: maxo}

    def _transform(self, request, record):
        locationId = record['monitoring_point_id']
        locationId = int(locationId)
        q = f"properties/source_id eq '{locationId}' and properties/agency eq '{AGENCY}'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'******* no location {locationId}')
        else:
            thing = self._client.get_thing(name=WATER_WELL['name'], location=loc['@iot.id'])
            if thing:
                try:
                    ds = self._client.get_datastream(name=GWL_DS['name'], thing=thing['@iot.id'])
                except StopIteration:
                    return

                if ds:
                    # get last observation for this datastream
                    eobs = self._client.get_observations(ds, limit=1,
                                                         pages=1,
                                                         verbose=False,
                                                         orderby='phenomenonTime desc')
                    last_obs = None
                    eobs = list(eobs)
                    if eobs:
                        last_obs = make_statime(eobs[0]['phenomenonTime'])
                    print(f'last obs datastream={ds} lastobs={last_obs} ')
                    vs = []
                    components = ['phenomenonTime', 'resultTime', 'result']
                    for obs in record['observations']:
                        dt = obs[self._timestamp_field]
                        if not dt:
                            print(f'skipping invalid datetime. {dt}')
                            continue

                        if not last_obs or (last_obs and dt > last_obs):
                            dt = datetime.datetime.utcfromtimestamp(dt/1000)
                            t = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                            v = obs[self._value_field]
                            try:
                                v = float(v)
                                vs.append((t, t, v))
                            except (TypeError, ValueError) as e:
                                print(f'skipping. error={e}. v={v}')

                    if vs:
                        payload = {'Datastream': asiotid(ds),
                                   'observations': vs,
                                   'components': components}
                        print('------------- payload', payload)
                        return payload


def etl_locations(request):
    stao = ISCSevenRiversLocationsSTAO()
    return stao.render(request)


def etl_things(request):
    stao = ISCSevenRiversThingsSTAO()
    return stao.render(request)


if __name__ == '__main__':
    # etl_things(None)
    stao = ISCSevenRiversWaterLevels()
    stao.render(None, True)
# ============= EOF =============================================
