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
from datetime import datetime
from io import BytesIO
from itertools import groupby

import httpx
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

from stao.base_stao import BucketSTAO, ObservationMixin, BaseSTAO, BQSTAO, LocationMixin, LocationGeoconnexMixin, \
    ThingMixin, DatastreamMixin, SimpleSTAO
from stao.ckan_stao import CKANResourceSTAO
from stao.constants import ENCODING_GEOJSON, WATER_WELL, NO_DESCRIPTION, DTW_OBS_PROP, ELEV_OBS_PROP, MANUAL_SENSOR, \
    WATER_QUANTITY, GWL_DS, GWE_DS, VAN_ESSEN_SENSOR
from stao.ebid.entities import EBIDGWLObservations
from stao.ose_roswell_basin.entities import CKANSTAO
from stao.util import make_geometry_point_from_latlon, copy_properties, asiotid, make_geometry_point_from_utm

import pandas as pd

class VanEssenSTAO(BQSTAO):
    _agency = 'SanAcaciaReach'
    _vocab_tag = 'van_essen'


class VanEssenSiteSTAO(VanEssenSTAO):
    _tablename = 'vanessen_sanacacia_reach_locations'
    _fields = ['id', 'uid', 'name', 'lat', 'lng', 'purpose', 'isActive', 'drillingDepth', 'numberOfScreens']

    _limit = 100
    _orderby = 'id asc'


class SanAcaciaReachLocations(LocationGeoconnexMixin, LocationMixin, VanEssenSiteSTAO):
    def _make_location_properties(self, record):
        properties = {
            'purpose': record['purpose'],
            'is_active': record['isActive'],
            'source_id': self.toST('location.properties.source_id', record),
            'number_of_screens': int(record['numberOfScreens']),
        }
        return properties


class SanAcaciaReachThings(ThingMixin, VanEssenSTAO):
    _tablename = 'vanessen_sanacacia_reach_monitoringpointlocations'
    _table_name_alias = 'ML'
    _fields = ['ML.name', 'ML.locationID', 'L.drillingDepth']
    _join = 'nmwdi.vanessen_sanacacia_reach_locations as L on L.id = locationID'

    def _transform(self, request, record):
        payload = super(SanAcaciaReachThings, self)._transform(request, record)
        if payload:
            payload['name'] = f"Groundwater Level Monitoring Point - {record['name']}"
        return payload

    def _get_location(self, record):
        name = int(record['locationID'])
        location = self._client.get_location(f"properties/source_id eq 'sanacaciareach-{name}'")
        return location

    def _make_thing_properties(self, record):
        properties = {
            'source_id': self.toST('thing.properties.source_id', record),
            'well_depth': {'value': self.toST('thing.properties.well_depth', record), 'unit': 'ft'},
            'nmbgmr_id': self.toST('thing.properties.nmbgmr_id', record),
        }
        return properties


class SanAcaciaReachDatastreams(DatastreamMixin, VanEssenSTAO):
    _tablename = 'vanessen_sanacacia_reach_monitoringpointlocations'
    _fields = ['id','name', 'locationID']
    _vocab_tag = 'van_essen'

    def _transform(self, request, record):
        payload = self._make_datastream_payload(record, 'gwl', self._agency)
        payload['properties'] = {'topic': WATER_QUANTITY,
                                 'agency': self._agency}
        return payload

    def _get_thing(self, record, agency):
        name = record['name']
        return self._client.get_thing(name=f'Groundwater Level Monitoring Point - {name}')


class SanAcaciaReachObservations(ObservationMixin, VanEssenSTAO):
    _tablename = 'vanessen_sanacacia_reach_monitoringpoints'
    _table_name_alias = 'MP'
    _fields = ['MP._airbyte_raw_id','L.name', 'monitoringPointID', 'ts', 'vrd']
    _join = 'nmwdi.vanessen_sanacacia_reach_monitoringpointlocations as L on L.id = monitoringPointID'
    _limit = 5000
    _location_field = 'name'
    _value_field = 'vrd'
    _timestamp_field = 'ts'

    _cursor_id = 'MP._airbyte_raw_id'
    _orderby = 'MP._airbyte_raw_id asc'

    _datastream_name = GWL_DS['name']

    _ground_surface_elevation = None
    def __init__(self):
        super(SanAcaciaReachObservations, self).__init__()
        self._ground_surface_elevation = {}

    def _extract_timestamp(self, dt):
        return int(dt)

    def _get_datastream(self, request, record):
        name = record['locationId']
        try:
            thing = self._client.get_thing(name=f"Groundwater Level Monitoring Point - {name}")
        except StopIteration:
            print("not thing found for ", name)
            return

        # get the ground surface elevation and store in a cache


        if thing:
            try:
                return self._client.get_datastream(thing=thing['@iot.id'], name=GWL_DS['name'])
            except StopIteration:
                print("not datastream found for ", name)
                return

    def _transform_value(self, v, record):
        gse = self._get_ground_surface_elevation(record)

        # convert mm to feet
        return (gse-v) * 0.00328084

    def _get_ground_surface_elevation(self, record):
        mid = record['monitoringPointID']
        ts = record['ts']
        ts = datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%dT%H:%M:%S')

        if mid in self._ground_surface_elevation:
            gse, fromDate = self._ground_surface_elevation[mid]
            if fromDate < ts:
                return gse

        sql = f'''select fromDate, elevation from nmwdi.vanessen_sanacacia_reach_groundsurfacedata 
        where monitoringPointID={mid} and fromDate<="{ts}" 
        order by fromDate desc'''

        results = self._bq_query(sql)
        if results:
            r = next(results)
            gse = float(r['elevation'])
            fromDate = r['fromDate']
            self._ground_surface_elevation[mid] = (gse, fromDate)
            return gse



    # def _get_thing_name(self, record):
    #     return f"Groundwater Level Monitoring Point - {record['name']}"
#
#     def _get_dataset_records(self, resource):
#         data = self._get_dataset(resource, 'content')
#         df = pd.read_excel(BytesIO(data))
#         print('extracting resource', resource)
#         return df.to_dict(orient='records')

    # def _extract_hook(self, dataset, records):
    #
    #     # yield {'resource': dataset, 'observations': records}
    #     def key(r):
    #         return
    #
    #
    #     for g, obs in groupby(sorted(records, key=key), key=key):
    #         obs = list(obs)
    #         yield {'observations': obs}
    #         # yield {'sys_loc_code': g, 'observations': obs}

    # def _transform(self, request, record):
    #     vs = []
    #     components = ['phenomenonTime', 'resultTime', 'result']
    #     # print('thiasd', ds)
    #     for obs in record['observations']:
    #         t = statime(obs[self._timestamp_field])
    #
    #         v = obs[self._attr]
    #         # parameters = {'measurement_method': obs['measurement_method'],
    #         #               'dry_indicator': obs['dry_indicator_yn'] }
    #         try:
    #             v = float(v)
    #             vs.append((t, t, v))
    #         except ValueError as e:
    #             print(f'skipping. error={e}. v={v}, attr={self._attr}')
    #
    #
    #     ds = {'@iot.id': 'foo'}
    #     # if ds:
    #     dtw = {'Datastream': asiotid(ds),
    #            'observations': vs,
    #            'components': components
    #            }
    #     return dtw
#


if __name__ == '__main__':

    # c = SimpleSTAO()
    # c.render('sensor', VAN_ESSEN_SENSOR)

    # c = SanAcaciaReachLocations()
    # c = SanAcaciaReachThings()
    # c = SanAcaciaReachDatastreams()
    c = SanAcaciaReachObservations()

    # c.render(None, dry=True)
    # c.render({'MP._airbyte_raw_id': 'ffffb0bc-c0b6-4bf7-bcbd-02163380b916', 'limit': None, 'counter': 1}, dry=True)
    # c.render(None, dry=False)
    c.render(None, dry=True)


    # c = EBWPCLocations()
    # c = EBWPCThings()
    # c = EBWPCDatastreams()
    # c = EBWPCLObservations()
    # c.render(None, dry=True)
    # c.render(None, dry=False)

# ============= EOF =============================================
