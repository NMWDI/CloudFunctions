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
from io import BytesIO
from itertools import groupby

import httpx
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

from stao.base_stao import BucketSTAO, ObservationMixin, BaseSTAO
from stao.ckan_stao import CKANResourceSTAO
from stao.constants import ENCODING_GEOJSON, WATER_WELL, NO_DESCRIPTION, DTW_OBS_PROP, ELEV_OBS_PROP, MANUAL_SENSOR, \
    WATER_QUANTITY, GWL_DS, GWE_DS
from stao.ebid.entities import EBIDGWLObservations
from stao.ose_roswell_basin.entities import CKANSTAO
from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid

import pandas as pd
AGENCY = 'EBWPC'


class EBWPCSTAO(CKANResourceSTAO):
    resource_id = '719844ad-46bf-46cf-a781-a111f114fe38'

    # for testing
    dataset_names = ('E-8428',)
    def _get_dataset_records(self, resource):
        data = self._get_dataset(resource, 'content')
        df = pd.read_excel(BytesIO(data))
        print('extracting resource', resource)
        return df.to_dict(orient='records')

    # def _extract_hook(self, records):
    #     yielded = []
    #     for row in records:
    #         name = row['sys_loc_code']
    #         if name in yielded:
    #             continue
    #
    #         if row['is_well'].strip().lower() != 'y':
    #             continue
    #
    #         yielded.append(name)
    #         yield row


class EBWPCLocations(EBWPCSTAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        pass
        # properties = {'agency': AGENCY,
        #               'elevation_reference': record['elevation_of'],
        #               'elevation_method': record['elev_collect_method_code'],
        #               'elevation_accuracy': record['elev_accuracy_value'],
        #               'elevation_datum': record['elev_datum_code'],
        #               'reference_point': record['reference_point'],
        #
        #               # 'facility_id': record['facility_id'],
        #               # 'facility_code': record['facility_code']
        #               }
        # lat, lon = float(record['lat']), float(record['long'])
        # if lat < 0:
        #     lat, lon = lon, lat
        #
        # elev = record['top_casing_elev'] or 0
        # elev = float(elev)
        #
        # # convert to meters
        # elev = elev * 0.3048
        # payload = {'name': record['sys_loc_code'],
        #            'description': record['loc_name'],
        #            'location': make_geometry_point_from_latlon(lat, lon, elevation=elev),
        #            "encodingType": ENCODING_GEOJSON,
        #            'properties': properties
        #            }
        # return payload


class EBWPCThings(EBWPCSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        def asfloat(attr):
            v = record[attr]
            try:
                return float(v)
            except ValueError:
                return 0


        # properties = {'agency': AGENCY,
        #               'well_depth': {'value': asfloat('depth_of_well'),
        #                              'unit':  record['well_depth_unit']},
        #
        #               'stickup_height': {'value': asfloat('stickup_height'),
        #                                'unit': record['stickup_unit']},
        #               'remark': record['well_remark'],
        #               'screens': [
        #                   {'ScreenTop': asfloat('start_depth'),
        #                    'ScreenBottom': asfloat('end_depth'),}
        #                 ],
        #               'casing_inner_diameter': asfloat('inner_diameter'),
        #               'casing_outer_diameter': asfloat('outer_diameter'),
        #               'casing_material': record['material_type_code'],
        #               'aquifer': record['aquifier'],
        #               }
        #
        #
        # name = record['sys_loc_code']
        # location = self._client.get_location(f"name eq '{name}'")
        # if location:
        #     payload = {'name': WATER_WELL['name'],
        #                'description': NO_DESCRIPTION,
        #                'properties': properties,
        #                'Locations': [asiotid(location)],
        #                }
        #     return payload
        # else:
        #     print(f'location {name} not found')

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


class EBWPCDatastreams(EBWPCSTAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        pass
        loc = self._client.get_location(f"name eq '{record['sys_loc_code']}' and properties/agency eq '{AGENCY}'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL['name'], location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
                welev_obsprop = next(self._client.get_observed_properties(name=ELEV_OBS_PROP['name']))
        #       sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))
        #
        #         thing_id = asiotid(thing)
        #         obsprop_id = asiotid(obsprop)
        #         welev_obsprop_id = asiotid(welev_obsprop)
        #         sensor_id = asiotid(sensor)
        #         properties = {'agency': AGENCY,
        #                       'topic': WATER_QUANTITY}
        #         dtw = {'name': GWL_DS['name'],
        #                'description': GWL_DS['description'],
        #                'Thing': thing_id,
        #                'ObservedProperty': obsprop_id,
        #                'Sensor': sensor_id,
        #                'unitOfMeasurement': FOOT,
        #                'observationType': OM_Measurement,
        #                'properties': properties
        #                }
        #         welev = {'name': GWE_DS['name'],
        #                  'description': GWE_DS['description'],
        #                  'Thing': thing_id,
        #                  'ObservedProperty': welev_obsprop_id,
        #                  'Sensor': sensor_id,
        #                  'unitOfMeasurement': FOOT,
        #                  'observationType': OM_Measurement,
        #                  'properties': properties
        #                  }
        #         payloads = [dtw, welev]
        #         return payloads


class EBWPCLObservations(EBWPCSTAO, ObservationMixin):
    _attr = 'transducer DTW, ft bgl'
    _name = GWL_DS['name']

    def _extract_hook(self, dataset, records):

        yield {'resource': dataset, 'observations': records}
        # for g, obs in groupby(sorted(reader, key=key), key=key):
        #     obs = list(obs)
        #     yield {'sys_loc_code': g, 'observations': obs}

    def _transform(self, request, record):
        vs = []
        components = ['phenomenonTime', 'resultTime', 'result']
        # print('thiasd', ds)
        for obs in record['observations']:
            t = statime(obs['date + time'])

            v = obs[self._attr]
            # parameters = {'measurement_method': obs['measurement_method'],
            #               'dry_indicator': obs['dry_indicator_yn'] }
            try:
                v = float(v)
                vs.append((t, t, v))
            except ValueError as e:
                print(f'skipping. error={e}. v={v}, attr={self._attr}')


        ds = {'@iot.id': 'foo'}
        # if ds:
        dtw = {'Datastream': asiotid(ds),
               'observations': vs,
               'components': components
               }
        return dtw

    # def _transform(self, request, record):
    #     loc = self._client.get_location(name=record['sys_loc_code'])
    #     if loc:
    #         thing = self._client.get_thing(name='Water Well', location=loc['@iot.id'])
    #         if thing:
    #             ds = self._client.get_datastream(name=self._name, thing=thing['@iot.id'])
    #
    #             vs = []
    #             components = ['phenomenonTime', 'resultTime', 'result']
    #             # print('thiasd', ds)
    #             for obs in record['observations']:
    #                 t = statime(obs['measurement_date'])
    #
    #                 v = obs[self._attr]
    #                 # parameters = {'measurement_method': obs['measurement_method'],
    #                 #               'dry_indicator': obs['dry_indicator_yn'] }
    #                 try:
    #                     v = float(v)
    #                     vs.append((t, t, v))
    #                 except ValueError as e:
    #                     print(f'skipping. error={e}. v={v}, attr={self._attr}')
    #
    #             if ds:
    #                 dtw = {'Datastream': asiotid(ds),
    #                        'observations': vs,
    #                        'components': components
    #                        }
    #                 return dtw


# class CABQWaterElevations(CABQObservations):
#     _attr = 'water_level'
#     _name = GWE_DS['name']
#
#
# class CABQWaterDepths(CABQObservations):
#     _attr = 'water_depth'
#     _name = GWL_DS['name']


if __name__ == '__main__':
    # c = CABQLocations()
    # c = CABQThings()

    # c = CABQSensors()
    # c = CABQObservedProperties()
    # c = CABQDatastreams()
    # c = CABQWaterElevations()
    c = EBWPCLObservations()
    c.render(None, dry=True)

# ============= EOF =============================================
