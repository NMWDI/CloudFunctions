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
import urllib.request
from itertools import groupby

import httpx
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

from stao.base_stao import BucketSTAO, ObservationMixin, BaseSTAO
from stao.ckan_stao import CKANResourceSTAO
from stao.constants import ENCODING_GEOJSON, WATER_WELL, NO_DESCRIPTION, DTW_OBS_PROP, ELEV_OBS_PROP, MANUAL_SENSOR, \
    WATER_QUANTITY, GWL_DS, GWE_DS
from stao.ose_roswell_basin.entities import CKANSTAO
from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid

# try:
#     from stao import BucketSTAO, ObservationMixin
#     from util import make_geometry_point_from_latlon, copy_properties, asiotid
#     from constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
#         ELEV_OBS_PROP, GWE_DS, WATER_QUANTITY
# except ImportError:
#     from stao.stao import BucketSTAO, ObservationMixin
#     from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid
#     from stao.constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
#         ELEV_OBS_PROP, GWE_DS, WATER_QUANTITY

AGENCY = 'CABQ'



class CABQSTAO(CKANResourceSTAO):
    dataset_names = 'Well Construction'
    resource_id = '8770b6eb-a958-4f2e-a901-f64f38ef25e9'
# class CABQSTAO(BucketSTAO):
#     _blobs = [
#               'cabq/COA_WaterLevels_All.txt',
#               'cabq/waterlevels3_11_2021-9_24-pm.txt',
#               'cabq/waterlevels3_22_2021-9_33-pm.txt',
#               'cabq/waterlevels5_5_2021-2_46-pm.txt',
#               ]

    # def _extract(self, request):
    #     print(f'extracting bucket {self._bucket}')
    #     bucket = self._get_bucket()
    #     for blob in self._blobs:
    #         print(f'extracting blob {blob}')
    #         blob = bucket.get_blob(blob)
    #         with blob.open() as rfile:
    #             reader = csv.DictReader(rfile, delimiter='\t')
    #             yield from self._extract_hook(reader)

    def _extract_hook(self, resource, records):
        yielded = []
        for row in records:
            name = row['sys_loc_code']
            if name in yielded:
                continue

            if row['is_well'].strip().lower() != 'y':
                continue

            yielded.append(name)
            yield row


class CABQLocations(CABQSTAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        properties = {'agency': AGENCY,
                      'elevation_reference': record['elevation_of'],
                      'elevation_method': record['elev_collect_method_code'],
                      'elevation_accuracy': record['elev_accuracy_value'],
                      'elevation_datum': record['elev_datum_code'],
                      'reference_point': record['reference_point'],

                      # 'facility_id': record['facility_id'],
                      # 'facility_code': record['facility_code']
                      }
        lat, lon = float(record['lat']), float(record['long'])
        if lat < 0:
            lat, lon = lon, lat

        elev = record['top_casing_elev'] or 0
        elev = float(elev)

        # convert to meters
        elev = elev * 0.3048
        payload = {'name': record['sys_loc_code'],
                   'description': record['loc_name'],
                   'location': make_geometry_point_from_latlon(lat, lon, elevation=elev),
                   "encodingType": ENCODING_GEOJSON,
                   'properties': properties
                   }
        return payload


class CABQThings(CABQSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        def asfloat(attr):
            v = record[attr]
            try:
                return float(v)
            except ValueError:
                return 0


        properties = {'agency': AGENCY,
                      'well_depth': {'value': asfloat('depth_of_well'),
                                     'unit':  record['well_depth_unit']},

                      'stickup_height': {'value': asfloat('stickup_height'),
                                       'unit': record['stickup_unit']},
                      'remark': record['well_remark'],
                      'screens': [
                          {'ScreenTop': asfloat('start_depth'),
                           'ScreenBottom': asfloat('end_depth'),}
                        ],
                      'casing_inner_diameter': asfloat('inner_diameter'),
                      'casing_outer_diameter': asfloat('outer_diameter'),
                      'casing_material': record['material_type_code'],
                      'aquifer': record['aquifier'],
                      }


        name = record['sys_loc_code']
        location = self._client.get_location(f"name eq '{name}'")
        if location:
            payload = {'name': WATER_WELL['name'],
                       'description': NO_DESCRIPTION,
                       'properties': properties,
                       'Locations': [asiotid(location)],
                       }
            return payload
        else:
            print(f'location {name} not found')

# use SimpleSTAO instead
# class CABQSensors(CABQSTAO):
#     _entity_tag = 'sensor'
#
#     def _transform(self, request, record):
#         return MANUAL_SENSOR
#
#
# class CABQObservedProperties(CABQSTAO):
#     _entity_tag = 'observed_property'
#
#     def _transform(self, request, record):
#         return [DTW_OBS_PROP, ELEV_OBS_PROP]


class CABQDatastreams(CABQSTAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        loc = self._client.get_location(f"name eq '{record['sys_loc_code']}' and properties/agency eq '{AGENCY}'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL['name'], location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
                welev_obsprop = next(self._client.get_observed_properties(name=ELEV_OBS_PROP['name']))
                sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))

                thing_id = asiotid(thing)
                obsprop_id = asiotid(obsprop)
                welev_obsprop_id = asiotid(welev_obsprop)
                sensor_id = asiotid(sensor)
                properties = {'agency': AGENCY,
                              'topic': WATER_QUANTITY}
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


class CABQObservations(CABQSTAO, ObservationMixin):
    _attr = None
    _name = None
    # resource_name = 'Water Levels'
    dataset_names = 'Water Levels'
    def _extract_hook(self, resource, records):
        def key(r):
            return r['sys_loc_code']

        for g, obs in groupby(sorted(records, key=key), key=key):
            obs = list(obs)
            yield {'sys_loc_code': g, 'observations': obs}

    def _transform(self, request, record):
        loc = self._client.get_location(name=record['sys_loc_code'])
        if loc:
            thing = self._client.get_thing(name='Water Well', location=loc['@iot.id'])
            if thing:
                ds = self._client.get_datastream(name=self._name, thing=thing['@iot.id'])

                vs = []
                components = ['phenomenonTime', 'resultTime', 'result', 'parameters']
                # print('thiasd', ds)
                for obs in record['observations']:
                    t = statime(obs['measurement_date'])

                    v = obs[self._attr]
                    parameters = {'measurement_method': obs['measurement_method'],
                                  'dry_indicator': obs['dry_indicator_yn'] }
                    try:
                        v = float(v)
                        vs.append((t, t, v, parameters))
                    except ValueError as e:
                        print(f'skipping. error={e}. v={v}, attr={self._attr}')

                if ds:
                    dtw = {'Datastream': asiotid(ds),
                           'observations': vs,
                           'components': components
                           }
                    return dtw


class CABQWaterElevations(CABQObservations):
    _attr = 'water_level'
    _name = GWE_DS['name']


class CABQWaterDepths(CABQObservations):
    _attr = 'water_depth'
    _name = GWL_DS['name']


if __name__ == '__main__':
    # c = CABQLocations()
    # c = CABQThings()

    # c = CABQSensors()
    # c = CABQObservedProperties()
    # c = CABQDatastreams()
    # c = CABQWaterElevations()
    c = CABQWaterDepths()
    # c.render(None, dry=True)
    c.render(None, dry=True)

# ============= EOF =============================================
