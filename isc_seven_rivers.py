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
    def render(self, request):
        resp = None
        data = self._extract(request)
        if data:
            data = self._transform(request, data)
            if data:
                resp = self._load(request, data)

        return resp

    def _extract(self, request):
        raise NotImplementedError

    def _transform(self, request, records):
        return records

    def _load(self, request, records):
        raise NotImplementedError

    def _get_bq_items(self, fields, dataset, tablename, where=None):
        client = bigquery.Client()
        sql = f'select {fields} from {dataset}.{tablename}'
        if where:
            sql = f'{sql} where {where}'

        job = client.query(sql)

        return job.result()


class ISCSevenRiversSTAO(BaseSTAO):
    def _extract(self, request):
        dataset = 'locations'
        tablename = 'isc_seven_rivers_sites'
        fields = []
        return self._get_bq_items(fields, dataset, tablename, where=None)

    def _load(self, request, records):
        for item in records:
            print(item)


def entrypoint(request):
    stao = ISCSevenRiversSTAO()
    return stao.render(request)


if __name__ == '__main__':
    entrypoint(None)
# ============= EOF =============================================
