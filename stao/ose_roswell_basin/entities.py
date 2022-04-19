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

import requests
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

try:
    from stao import BucketSTAO, ObservationMixin, BaseSTAO
    from util import make_geometry_point_from_latlon, copy_properties, asiotid
    from constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
        ELEV_OBS_PROP, GWE_DS, WATER_QUANTITY
except ImportError:
    from stao.stao import BucketSTAO, ObservationMixin, BaseSTAO
    from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid
    from stao.constants import DTW_OBS_PROP, NO_DESCRIPTION, WATER_WELL, ENCODING_GEOJSON, MANUAL_SENSOR, GWL_DS, \
        ELEV_OBS_PROP, GWE_DS, WATER_QUANTITY

AGENCY = 'OSE-Roswell'


class CKANSTAO(BaseSTAO):
    resource_id = ''
    ckan_url = ''
    def _get_dict_iter(self):
        blob = self._get_blob()
        header = None

        for line in blob.split('\n'):
            line = line.strip()
            if line:
                line = line.split(',')

                if not header:
                    header = [h.lower() for h in line]
                    continue
                yield dict(zip(header, line))

    def _extract(self, request):
        yielded = []
        for record in self._get_dict_iter():
            record = self._extract_hook(yielded, record)
            if record:
                yield record

    def _extract_hook(self, yielded, record):
        return record

    def _get_blob(self):
        url = f'{self.ckan_url}/datastore/dump/{self.resource_id}'
        resp = requests.get(url)
        return resp.text


class OSERoswellSTAO(CKANSTAO):
    ckan_url = 'https://catalog.newmexicowaterdata.org/'

    def _extract_hook(self, yielded, record):
        if record['site_id'] not in yielded:
            yielded.append(record['site_id'])
            return record


# {'_id': '983', 'site_id': '334424 104193601', 'date': '2020-01-22T00:00:00', 'time': '1899-12-30T00:00:00', 'location': '7S.26E.6.242343', 'dd_lat': '33.739556', 'dd_lon': '-104.329750', 'dms_lat': '33 44 22.4', 'dms_lon': '104 19 47.1', 'dtwgs': '23.600000', 'basin': 'Roswell', 'comment': '""', 'utm_east': '562085', 'utm_north': '3733480'}

class OSERoswellLocations(OSERoswellSTAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        properties = {'agency': AGENCY,
                      'basin': record.get('basin')
                      }
        lat, lon = float(record['dd_lat']), float(record['dd_lon'])
        if lat < 0:
            lat, lon = lon, lat

        payload = {
            'name': record['site_id'],
            'description': NO_DESCRIPTION,
            'location': make_geometry_point_from_latlon(lat, lon),
            "encodingType": ENCODING_GEOJSON,
            'properties': properties
        }
        return payload


class OSERoswellThings(OSERoswellSTAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        properties = {'agency': AGENCY}
        name = record['site_id']
        location = self._client.get_location(f"name eq '{name}'")
        payload = {'name': WATER_WELL,
                   'description': NO_DESCRIPTION,
                   'properties': properties,
                   'Locations': [asiotid(location)],
                   }
        return payload



class OSERoswellDatastreams(OSERoswellSTAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        loc = self._client.get_location(f"name eq '{record['site_id']}' and properties/agency eq '{AGENCY}'")
        if loc:
            lid = loc['@iot.id']

            thing = self._client.get_thing(name=WATER_WELL, location=lid)
            if thing:
                obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
                sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))

                thing_id = asiotid(thing)
                obsprop_id = asiotid(obsprop)
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
                return dtw


class OSERoswellObservations(OSERoswellSTAO, ObservationMixin):
    def _extract(self, request):
        def key(r):
            return r['site_id']

        ds = list(self._get_dict_iter())
        for site_id, gs in groupby(sorted(ds, key=key), key=key):
            yield {'site_id': site_id, 'observations': gs}

    def _transform(self, request, record):
        loc = self._client.get_location(name=record['site_id'])
        if loc:
            thing = self._client.get_thing(name=WATER_WELL, location=loc['@iot.id'])
            if thing:
                ds = self._client.get_datastream(name=GWL_DS['name'], thing=thing['@iot.id'])

                vs = []
                components = ['phenomenonTime', 'resultTime', 'result']
                for obs in record['observations']:
                    da = obs['date'].split('T')[0]
                    ti = obs['time'].split('T')[1]
                    t = f'{da}T{ti}.000Z'
                    v = obs['dtwgs']
                    try:
                        v = float(v)
                        vs.append((t, t, v))
                    except ValueError as e:
                        print(f'skipping. error={e}. v={v}, attr={self._attr}')

                if ds:
                    dtw = {'Datastream': asiotid(ds),
                           'observations': vs,
                           'components': components
                           }
                    return dtw


# class OSERoswellWaterElevations(OSERoswellObservations):
#     _attr = 'water_level'
#     _name = GWE_DS['name']
#
#
# class OSERoswellWaterDepths(OSERoswellObservations):
#     _attr = 'water_depth'
#     _name = GWL_DS['name']


if __name__ == '__main__':

    # c = Roswell()
    # c.render(None, dry=False)
    #
    # c = FTSumner()
    # c.render(None, dry=False)
    #
    # c = Hondo()
    # c.render(None, dry=False)
    resources = (('Roswell', '5f64d411-c281-491e-ae12-03280c248112'),
                 ('FTSumner', '3fa1cd2c-be33-4bba-a65b-bbc786dcbd39'),
                 ('Hondo', 'ce18fbb9-296d-4b40-ba66-f81a061051ac'))

    dry = False
    # dry = True
    for name, rid in resources:
        # c = OSERoswellLocations()
        # c = OSERoswellThings()
        # c = OSERoswellDatastreams()
        c = OSERoswellObservations()
        c.resource_id = rid
        c.render(None, dry=dry)

    # c = CABQLocations()
    # c = CABQThings()

    # c = CABQSensors()
    # c = CABQObservedProperties()
    # c = OSERoswellDatastreams()
    # c = CABQWaterElevations()
    # c.render(None)

# ============= EOF =============================================
