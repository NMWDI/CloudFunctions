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
import csv
from itertools import groupby

from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

try:
    from stao import BucketSTAO, ObservationMixin
    from util import make_geometry_point_from_latlon, copy_properties, asiotid
    from constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
        ELEV_OBS_PROP, GWE_DS
except ImportError:
    from stao.stao import BucketSTAO, ObservationMixin
    from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid
    from stao.constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
        ELEV_OBS_PROP, GWE_DS

AGENCY = 'CABQ'


class CABQSTAO(BucketSTAO):
    _blobs = ['cabq/COA_WaterLevels_All.txt',
              'cabq/waterlevels3_11_2021-9_24-pm.txt',
              'cabq/waterlevels3_22_2021-9_33-pm.txt',
              'cabq/waterlevels5_5_2021-2_46-pm.txt',
              ]

    def _extract(self, request):
        print(f'extracting bucket {self._bucket}')
        bucket = self._get_bucket()
        for blob in self._blobs:
            print(f'extracting blob {blob}')
            blob = bucket.get_blob(blob)
            with blob.open() as rfile:
                reader = csv.DictReader(rfile, delimiter='\t')
                yield from self._extract_hook(reader)

    def _extract_hook(self, reader):
        yielded = []
        for row in reader:
            name = row['sys_loc_code']
            if name in yielded:
                continue

            yielded.append(name)
            yield row


class CABQLocations(CABQSTAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        properties = {'agency': AGENCY,
                      'altitude': record['reference_elev'],
                      'altitude_units': 'feet asl',
                      'facility_id': record['facility_id'],
                      'facility_code': record['facility_code']
                      }
        lat, lon = float(record['latitude']), float(record['longitude'])
        if lat < 0:
            lat, lon = lon, lat

        payload = {'name': record['sys_loc_code'],
                   'description': record['loc_name'],
                   'location': make_geometry_point_from_latlon(lat, lon),
                   "encodingType": ENCODING_GEOJSON,
                   'properties': properties
                   }
        return payload


class CABQThings(CABQSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        properties = {'agency': AGENCY}
        copy_properties(properties, record, ('measured_depth_of_well',
                                             'lnapl_cas_rn',
                                             'lnapl_depth',
                                             'lnapl_thickness',
                                             'lnapl_density',
                                             'dnapl_cas_rn',
                                             'dnapl_depth',
                                             'dnapl_thickness',
                                             ))

        name = record['sys_loc_code']
        location = self._client.get_location(f"name eq '{name}'")
        payload = {'name': WATER_WELL,
                   'description': NO_DESCRIPTION,
                   'properties': properties,
                   'Locations': [asiotid(location)],
                   }
        return payload


class CABQSensors(CABQSTAO):
    _entity_tag = 'sensor'

    def _transform(self, request, record):
        return MANUAL_SENSOR


class CABQObservedProperties(CABQSTAO):
    _entity_tag = 'observed_property'

    def _transform(self, request, record):
        return [DTW_OBS_PROP, ELEV_OBS_PROP]


class CABQDatastreams(CABQSTAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        loc = self._client.get_location(f"name eq '{record['sys_loc_code']}' and properties/agency eq '{AGENCY}'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL, location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
                welev_obsprop = next(self._client.get_observed_properties(name=ELEV_OBS_PROP['name']))
                sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))

                thing_id = asiotid(thing)
                obsprop_id = asiotid(obsprop)
                welev_obsprop_id = asiotid(welev_obsprop)
                sensor_id = asiotid(sensor)
                properties = {}
                dtw = {'name': GWL_DS['name'],
                       'description': GWL_DS['description'],
                       'Thing': thing_id,
                       'ObservedProperty': obsprop_id,
                       'Sensor': sensor_id,
                       'unitOfMeasurement': FOOT,
                       'observationType': OM_Measurement,
                       'properties': properties
                       }
                welev = {'name': GWE_DS['name'],
                         'description': GWE_DS['description'],
                         'Thing': thing_id,
                         'ObservedProperty': welev_obsprop_id,
                         'Sensor': sensor_id,
                         'unitOfMeasurement': FOOT,
                         'observationType': OM_Measurement,
                         'properties': properties
                         }
                payloads = [dtw, welev]
                return payloads


class CABQWaterLevels(CABQSTAO, ObservationMixin):

    def _extract_hook(self, reader):
        def key(r):
            return r['sys_loc_code']

        for g, obs in groupby(sorted(reader, key=key), key=key):
            yield {'sys_loc_code': g, 'observations': obs}

    def _transform(self, request, record):
        loc = self._client.get_location(name=record['sys_loc_code'])
        if loc:
            thing = self._client.get_thing(name='Water Well', location=loc['@iot.id'])
            if thing:
                ds = self._client.get_datastream(name=GWL_DS['name'], thing=thing['@iot.id'])

                dtws = []
                welevs = []
                components = ['phenomenonTime', 'resultTime', 'result']
                for obs in record['observations']:
                    t = statime(obs['measurement_date'])
                    for attr, vs in (('water_depth', dtws), ('water_level', welevs)):
                        v = obs['water_depth']
                        try:
                            v = float(v)
                            vs.append((t, t, v))
                        except ValueError as e:
                            print(f'skipping. error={e}. v={v}, attr={attr}')
                payloads = []
                if ds:
                    dtw = {'Datastream': asiotid(ds),
                           'observations': dtws,
                           'components': components
                           }
                    payloads.append(dtw)

                ds = self._client.get_datastream(name=GWE_DS['name'], thing=thing['@iot.id'])
                if ds:
                    welev = {'Datastream': asiotid(ds),
                             'observations': welevs,
                             'components': components
                             }
                    payloads.append(welev)
                return payloads


if __name__ == '__main__':
    # c = CABQLocations()
    # c = CABQThings()

    # c = CABQSensors()
    # c = CABQObservedProperties()
    # c = CABQDatastreams()
    c = CABQWaterLevels()
    c.render(None)

# ============= EOF =============================================
