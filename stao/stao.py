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
        obj = func(record)
        print(f'     iotid={obj.iotid}')
        return obj


class BQSTAO(BaseSTAO):
    _fields = None
    _dataset = None
    _tablename = None

    def _extract(self, request):
        return self._get_bq_items(self._fields, self._dataset, self._tablename, where=None)

    def _bq_query(self, sql, **kw):
        client = bigquery.Client()
        job = client.query(sql, **kw)
        return job.result()

    def _get_bq_items(self, fields, dataset, tablename, where=None):
        fs = ','.join(fields)
        sql = f'select {fs} from {dataset}.{tablename}'
        # if where:
        #     sql = f'{sql} where {where}'

        return self._bq_query(sql)


class LocationGeoconnexMixin:
    def _load_record(self, record):
        obj = super(LocationGeoconnexMixin, self)._load_record(record)
        iotid = obj.iotid
        props = record['properties']
        props['geoconnex'] = f'https://geoconnex.us/nmwdi/st/locations/{iotid}'

        payload = {'properties': props}
        self._client.patch_location(iotid, payload)
# ============= EOF =============================================