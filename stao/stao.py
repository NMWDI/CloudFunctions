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
import json

from google.cloud import bigquery, storage

try:
    from util import make_sta_client
except ImportError:
    from stao.util import make_sta_client


class ObservationMixin:
    def _get_load_function_name(self):
        return 'add_observations'


class SimpleSTAO:
    def render(self, tag, payload):
        client = make_sta_client()

        func = getattr(client, f'put_{tag}')
        func(payload)


class BaseSTAO:
    _limit = None

    def __init__(self):
        self._client = make_sta_client()
        self.state = {}

    def render(self, request, dry=False):
        if request:
            if request.json:
                self.state = request.json

        data = self._extract(request)
        if data:
            resp = self._load(request, data, dry)
        else:
            resp = self.state

        return resp

    def _extract(self, request):
        raise NotImplementedError

    def _transform(self, request, record):
        return record

    def _transform_message(self, record):
        return record

    def _load(self, request, records, dry):
        cnt = 0
        for i, record in enumerate(records):
            print(f'transform record {i} {self._transform_message(record)}')
            payloads = self._transform(request, record)
            if payloads:
                if not isinstance(payloads, (tuple, list)):
                    payloads = (payloads,)

                for payload in payloads:
                    self._load_record(payload, dry)
                    cnt += 1
            else:
                print(f'skipping {record}')

        if record:
            state = {'OBJECTID': record['OBJECTID'],
                     'limit': self._limit}
            self.state = state

        return self.state

    def _load_record(self, payload, dry):
        clt = self._client

        if hasattr(self, '_get_load_function_name'):
            funcname = self._get_load_function_name()
        else:
            tag = self._entity_tag
            funcname = f'put_{tag.lower()}'

        func = getattr(clt, funcname)

        # print(f'calling {funcname} {func} {record}')
        # print(f'dry={dry} load record={record}')
        obj = func(payload, dry=dry)

        # print(f'     iotid={obj.iotid}')
        return obj


class BQSTAO(BaseSTAO):
    _fields = None
    _dataset = None
    _tablename = None
    _orderby = None

    def _extract(self, request):
        print('request {} {}'.format(request, request.json if request else 'no json'))
        try:
            where = request.json.get('where')
        except (ValueError, AttributeError) as e:
            print('error a {}'.format(e))
            where = None

        if not where:
            try:
                obj = int(request.json.get('OBJECTID'))
                where = f"OBJECTID>{obj}"
            except (ValueError, AttributeError, TypeError) as e:
                print('error b {}'.format(e))
                where = None

        try:
            self._limit = int(request.json.get('limit'))
        except (ValueError, AttributeError):
            pass

        print('where {} {}'.format(where, self._limit))
        return self._handle_extract(self._get_bq_items(self._fields, self._dataset, self._tablename, where=where))

    def _bq_query(self, sql, **kw):
        client = bigquery.Client()
        job = client.query(sql, **kw)
        return job.result()

    def _get_bq_items(self, fields, dataset, tablename, where=None):
        fs = ','.join(fields)
        sql = f'select {fs} from {dataset}.{tablename}'
        if where:
            sql = f'{sql} where {where}'

        if self._orderby:
            sql = f'{sql} order by {self._orderby}'

        if self._limit:
            sql = f'{sql} limit {self._limit}'

        return self._bq_query(sql)

    def _handle_extract(self, records):
        return records


class LocationGeoconnexMixin:
    def _load_record(self, record):
        obj = super(LocationGeoconnexMixin, self)._load_record(record)
        iotid = obj.iotid
        props = record['properties']
        props['geoconnex'] = f'https://geoconnex.us/nmwdi/st/locations/{iotid}'

        payload = {'properties': props}
        self._client.patch_location(iotid, payload)


class BucketSTAO(BaseSTAO):
    _bucket = 'waterdatainitiative'
    _blob = None

    def _get_bucket(self):
        client = storage.Client()
        bucket = client.get_bucket(self._bucket)
        return bucket

    def _extract(self, request):
        print(f'extracting bucket {self._bucket}')
        bucket = self._get_bucket()
        blob = bucket.get_blob(self._blob)
        jobj = json.loads(blob.download_as_bytes())
        return self._handle_extract(jobj)

    def _handle_extract(self, jobj):
        return jobj
# ============= EOF =============================================
