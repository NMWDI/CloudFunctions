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
# from datetime import datetime
from io import BytesIO
from itertools import groupby
import datetime
import httpx
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

from stao.base_stao import BucketSTAO, ObservationMixin, BaseSTAO, SimpleSTAO
from stao.ckan_stao import CKANResourceSTAO
from stao.constants import ENCODING_GEOJSON, WATER_WELL, NO_DESCRIPTION, DTW_OBS_PROP, ELEV_OBS_PROP, MANUAL_SENSOR, \
    WATER_QUANTITY, GWL_DS, GWE_DS, MANUAL_GWL_DS, TRANSDUCER_SENSOR
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

    # def _get_dataset_records(self, resource):
    #     data = self._get_dataset(resource, 'content')
    #     df = pd.read_csv(BytesIO(data))
    #     print('extracting resource', resource)
    #     return df.to_dict(orient='records')


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
                properties = {'agency': AGENCY,
                              'topic': WATER_QUANTITY}

                manual_payload = {'name': MANUAL_GWL_DS['name'],
                       'description': MANUAL_GWL_DS['description'],
                       'Thing': thing_id,
                       'ObservedProperty': obsprop_id,
                       'Sensor': asiotid(sensor),
                       'unitOfMeasurement': FOOT,
                       'observationType': OM_Measurement,
                       'properties': properties
                       }

                sensor = next(self._client.get_sensors(name=TRANSDUCER_SENSOR['name']))
                properties = {'agency': AGENCY, 'topic': WATER_QUANTITY, 'is_continuous': True}

                continuous_payload = {'name': GWL_DS['name'],
                                      'description': GWL_DS['description'],
                                      'Thing': thing_id,
                                       'ObservedProperty': obsprop_id,
                                       'Sensor': asiotid(sensor),
                                       'unitOfMeasurement': FOOT,
                                       'observationType': OM_Measurement,
                                       'properties': properties
                                       }

                return [manual_payload, continuous_payload]


class EBWPCObservations(ObservationMixin, EBWPCSTAO):
    def __init__(self, *args, **kw):
        super(EBWPCObservations, self).__init__(*args, **kw)
        self.dataset_names = None
        self.selected_name = None

    def excluded_dataset_names(self, record):
        name = record['name']
        # ret = record['name'] == 'EBWPC Well Locations'


        excluded = name == 'EBWPC Well Locations' #or name.lower().endswith('archived')
        #
        # excluded = excluded or name != 'Osita Ranch'
        # excluded = excluded or name != 'E-8428'
        # excluded = excluded or name != 'Smith-1'
        if self.selected_name:
            excluded = excluded or name != self.selected_name

        return excluded

    def _get_location(self, record, location_id=None):
        location_id = record['resource']['name']
        q = f"name eq '{location_id}' and properties/agency eq '{AGENCY}'"
        return self._client.get_location(query=q), location_id

    def _extract_hook(self, dataset, records):
        try:
            records = [r for r in records if r[self._value_field] and r[self._value_field] != '#REF!' and r[self._value_field]!='N/A']
        except KeyError as e:
            record = next(records)
            print('failed to extract', e, record.keys())
            return

        if self._limit:
            records = records[:self._limit]
        yield {'resource': dataset, 'observations': records}


class EBWPCManualObservations(EBWPCObservations):
    _datastream_name = MANUAL_GWL_DS['name']
    _value_field = 'manual_depth_bgs'

    def _get_timestamp(self, obs):
        md = obs['measurement_date']
        mt = obs['measurement_time']
        if mt and mt != 'N/A':
            return f'{md} {mt}'
        return md

    def _transform_timestamp(self, dt):
        errors = []
        for fmt in ('%m/%d/%y %I:%M',
                    '%m/%d/%y %I:%M:%S %p',
                    '%m/%d/%Y %I:%M:%S %p',
                    '%m/%d/%Y %H:%M'):
            try:
                dt = datetime.datetime.strptime(dt, fmt)
                dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except ValueError as e:
                errors.append(e)
        else:
            print('failed to parse', errors, dt)


class EBWPCContinuousObservations(EBWPCObservations):
    _datastream_name = GWL_DS['name']
    _value_field = 'transducer_depth_bgs'

    # def _get_timestamp(self, obs):
    #     md = obs['measurement_date']
    #     mt = obs['measurement_time']
    #     return f'{md} {mt}'





if __name__ == '__main__':

    # ss = SimpleSTAO()
    # ss.render('sensor', TRANSDUCER_SENSOR)
              # c = EBWPCLocations()
    # c = EBWPCThings()

    # c = EBWPCDatastreams()
    # c = EBWPCManualObservations()
    c = EBWPCContinuousObservations()
    # c.selected_name = 'E-10652'
    # c.selected_name = 'E-7545'
    # c.selected_name = 'Magnum Steel' no transducer data
    # c.selected_name = 'E-1639-POD1'
    # c.selected_name = 'T-6363'
    # c.selected_name = 'E-00428'
    # c.selected_name = 'E-50-4'
    # c.selected_name = 'E-2034' # no location
    # c.selected_name = 'E-2298'
    # c.selected_name ='E-9673' # no location
    # c.selected_name = 'E-50-1-Archived' # no location
    # c.selected_name = 'E-6385-Archived' #  uses manual_depth_Below_TOC
    # c.selected_name = 'Greene-4-Archived' # no location
    # c.selected_name = 'Hagerman HQ-Archived' # uses manual_depth_BTOC
    # c.selected_name = 'Lujan-1-Archived' # no location
    #c.selected_name = 'Ruby Shaw WM-Archived' # uses manual_depth_BTOC
    #c._limit = 10

    c.render(None, dry=True)
    # c.render(None, dry=False)

# ============= EOF =============================================
