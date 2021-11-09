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
import requests
from google.cloud import bigquery

from util import make_sta_client


class BaseSTAO:
    entity_tag = None

    def __init__(self):
        self._client = make_sta_client()

    def render(self, request):
        resp = None
        data = self._extract(request)
        if data:
            resp = self._load(request, data)

        return resp

    def _extract(self, request):
        raise NotImplementedError

    def _transform(self, request, record):
        return record

    def _load(self, request, records):
        cnt = 0
        for record in records:
            record = self._transform(request, record)
            self._load_record(record)
            cnt += 1
        return f'Loaded {cnt} records'

    def _load_record(self, record):
        tag = self._entity_tag
        clt = self._client
        funcname = f'put_{tag.lower()[:-1]}'
        func = getattr(clt, funcname)
        # print(f'calling {funcname} {func} {record}')
        print(f'load record={record}')
        iotid = func(record).iotid
        print(f'     iotid={iotid}')

    def _get_bq_items(self, fields, dataset, tablename, where=None):
        client = bigquery.Client()
        fs = ','.join(fields)
        sql = f'select {fs} from {dataset}.{tablename}'
        if where:
            sql = f'{sql} where {where}'

        job = client.query(sql)

        return job.result()


class ISCSevenRiversLocationsSTAO(BaseSTAO):
    _entity_tag = 'Locations'

    def _extract(self, request):
        dataset = 'locations'
        tablename = 'isc_seven_rivers_monitoring_points'
        fields = ['id', 'name', 'type', 'comments', 'latitude', 'longitude', 'groundSurfaceElevationFeet']
        return self._get_bq_items(fields, dataset, tablename, where=None)

    def _transform(self, request, record):
        """
        return a ST compitable object
        :param request:
        :param records:
        :return:
        """

        loc = {"type": "Point", "coordinates": [record['longitude'],
                                                record['latitude']]}
        props = {'source_id': record['id'],
                 'agency': 'ISC_SEVEN_RIVERS',
                 'groundSurfaceElevationFeet': record['groundSurfaceElevationFeet']}
        obj = {'name': record['name'],
               'description': record['comments'] or 'No Description',
               'location': loc,
               'properties': props,
               "encodingType": "application/vnd.geo+json", }

        return obj


class ISCSevenRiversThingsSTAO(BaseSTAO):
    entity_tag = 'Things'

    def _extract(self, request):
        dataset = 'locations'
        tablename = 'isc_seven_rivers_sites'
        fields = []
        return self._get_bq_items(fields, dataset, tablename, where=None)

    def _transform(self, request, record):
        location_id = self._client.get_location_id(record['name'])
        props = {}
        obj = {'name': record['name'],
               'description': 'No Description',
               'properties': props,
               'Locations': [{'@iot.id': location_id}]}
        return obj


def etl_locations(request):
    stao = ISCSevenRiversLocationsSTAO()
    return stao.render(request)


def etl_things(request):
    stao = ISCSevenRiversThingsSTAO()
    return stao.render(request)


if __name__ == '__main__':
    etl_locations(None)
# ============= EOF =============================================
