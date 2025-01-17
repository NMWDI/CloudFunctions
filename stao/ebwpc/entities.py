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
from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid, make_geometry_point_from_utm

import pandas as pd
AGENCY = 'EBWPC'


class EBWPCSTAO(CKANResourceSTAO):
    resource_id = '719844ad-46bf-46cf-a781-a111f114fe38'

    # for testing
    # dataset_names = ('E-8428',)

    dataset_names = ('EBWPC Well Locations',)

    def _get_dataset_records(self, resource):
        data = self._get_dataset(resource, 'content')
        df = pd.read_csv(BytesIO(data))
        print('extracting resource', resource)
        return df.to_dict(orient='records')


class EBWPCLocations(EBWPCSTAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        if record['NMBG_ID'].endswith('archived'):
            return

        properties = {'agency': AGENCY,}

        payload = {'name': record['NMBG_ID'],
                   'description': record['NMBG_ID'],
                   'location': make_geometry_point_from_utm(float(record['UTM_Zone13N_Easting']),
                                                            float(record['UTM_Zone13N_Northing']),
                                                            ellps='GRS80',
                                                            zone=13),
                   "encodingType": ENCODING_GEOJSON,
                   'properties': properties
                   }
        return payload


class EBWPCThings(EBWPCSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        properties = {'agency': AGENCY}
        name = record['NMBG_ID']
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


class EBWPCDatastreams(EBWPCSTAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):

        loc = self._client.get_location(f"name eq '{record['NMBG_ID']}' and properties/agency eq '{AGENCY}'")

        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL['name'], location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
                sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))

                thing_id = asiotid(thing)
                obsprop_id = asiotid(obsprop)
                sensor_id = asiotid(sensor)
                properties = {'agency': AGENCY,
                              'topic': WATER_QUANTITY}

                payload = {'name': GWL_DS['name'],
                       'description': GWL_DS['description'],
                       'Thing': thing_id,
                       'ObservedProperty': obsprop_id,
                       'Sensor': sensor_id,
                       'unitOfMeasurement': FOOT,
                       'observationType': OM_Measurement,
                       'properties': properties
                       }

                return payload


class EBWPCLObservations(EBWPCSTAO, ObservationMixin):
    _attr = 'transducer DTW, ft bgl'
    _name = GWL_DS['name']

    def _get_dataset_records(self, resource):
        data = self._get_dataset(resource, 'content')
        df = pd.read_excel(BytesIO(data))
        print('extracting resource', resource)
        return df.to_dict(orient='records')

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



if __name__ == '__main__':

    # c = EBWPCLocations()
    # c = EBWPCThings()
    c = EBWPCDatastreams()
    # c = EBWPCLObservations()
    c.render(None, dry=True)
    # c.render(None, dry=False)

# ============= EOF =============================================
