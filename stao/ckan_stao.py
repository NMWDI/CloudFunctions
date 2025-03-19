# ===============================================================================
# Copyright 2024 ross
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
import json

import httpx

from stao.base_stao import BaseSTAO


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
        url = f'https://catalog.newmexicowaterdata.org/api/3/action/resource_show?id={self.resource_id}'
        # url = f'{self.ckan_url}datastore/dump/{self.resource_id}'
        # print(url)
        resp = httpx.get(url)
        # print(resp)
        # print(resp.json())
        resp = httpx.get(resp.json()['result']['url'])
        return resp.text


class CKANResourceSTAO(BaseSTAO):
    dataset_names = None
    resource_id = None
    excluded_dataset_names = None

    def _get_datasets(self):
        url = f'https://catalog.newmexicowaterdata.org/api/3/action/package_show?id={self.resource_id}'
        resp = httpx.get(url, follow_redirects=True)
        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(resp.url, resp.text)
            return []

        resources =  data['result']['resources']
        dataset_names = self.dataset_names
        if dataset_names:
            if not isinstance(dataset_names, (list, tuple)):
                dataset_names = (dataset_names,)
        if dataset_names:
            resources = [r for r in resources if r['name'] in dataset_names]

        excluded_dataset_names = self.excluded_dataset_names
        if excluded_dataset_names:
            if callable(excluded_dataset_names):
                resources = [r for r in resources if not excluded_dataset_names(r)]
            else:
                resources = [r for r in resources if r['name'] not in excluded_dataset_names]

        return resources

    def _extract(self, request):

        for dataset in self._get_datasets():
            print(dataset['name'])
            records = self._get_dataset_records(dataset)
            yield from self._extract_hook(dataset, records)

        # for resource in self._get_resources():
        #     records = self._get_resource_records(resource)
        #
        # yield from self._extract_hook(records)

    def _get_dataset(self, dataset, attr ='text'):
        url = dataset['url']
        resp = httpx.get(url, follow_redirects=True)
        # print(resp.text)
        return getattr(resp, attr)

    def _get_dataset_records(self, dataset):
        data = self._get_dataset(dataset)
        reader = csv.DictReader(data.split('\n'), delimiter=',')
        return reader

    def _extract_hook(self, dataset, records):
        return records

# ============= EOF =============================================
