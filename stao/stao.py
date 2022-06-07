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
    """
    Observation mixin class.
    """
    def _get_load_function_name(self):
        return 'add_observations'


class STAO:
    """
    Base class for all SensorThings Access Objects
    here to enforce that subclasses implement a render method
    """
    def render(self, *args, **kw):
        raise NotImplementedError


class SimpleSTAO(STAO):
    """
    Simple SensorThings Access Object

    use this to add explicit payloads to ST. It is a simple wrapper around a pysta.Client object.

    example usage:
    ss = SimpleSTAO()
    ss.render('sensor', MANUAL_SENSOR)  # where MANUAL_SENSOR is dict representing a JSON payload
    ss.render('observed_property', DTW_OBS_PROP) # where DTW_OBS_PROP is dict representing a JSON payload


    MANUAL_SENSOR = {'name': 'Manual',
                 'description': NO_DESCRIPTION,
                 'encodingType': ENCODING_PDF,
                 'metadata': NO_METADATA
                 }
    """

    def render(self, tag, payload):
        """
        use this method to explicitly upload an entity

        :param tag: str.  name of the ST entity e.g "sensor"
        :param payload: dict.  JSON-style payload for the entity
        :return: None
        """
        client = make_sta_client()

        func = getattr(client, f'put_{tag}')
        func(payload)


class BaseSTAO(STAO):

    """
    Base class for all more advanced STAOs.

    all subclasses must implement an _extract(self, request) method
    all subclasses must define _entity_tag.  e.g _entity_tag = "location"

    optionally the subclass may implement a _transform(self, request, record) method

    """
    _limit = None
    _entity_tag = None
    _cursor_id = 'OBJECTID'

    def __init__(self):
        """
        """
        self._client = make_sta_client()
        self.state = {}

    def render(self, request, dry=False):
        """

        :param request: Request object passed in by the CloudFunction trigger
        :param dry: optional keyword for testing.  dry=False goes through the motions but does not send POSTs to the
        ST server
        :return: dict.  return the STAOs state
        """

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
        """
        return a transformed record
        :param request:  Request object passed in by the CloudFunction trigger
        :param record:  dict representing an individual "record"
        :return:
        """

        return record

    def _transform_message(self, record):
        """
        Override this method to modify how the record is printed to std out.

        by default the record is returned unmodified

        this function can return any "printable" object
        :param record:
        :return: record
        """
        return record

    def _load(self, request, records, dry):
        """
        Load a list of records to an ST instance

        :param request: Request object passed in by the CloudFunction trigger
        :param records: list of records
        :param dry: flag for testing. if true goes through the motions but does not send POSTs to the
        ST server
        :return:
        """
        cnt = 0
        counter = self.state.get('counter', 0)

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
                print(f'        skipping {record}')
            print('-----------------------------------------------')

            state = {self._cursor_id: record.get(self._cursor_id),
                     'limit': self._limit,
                     }
            self.state = state
        self.state['counter'] = counter+1
        return self.state

    def _load_record(self, payload, dry):
        """
        Uses the pysta.Client to POST a payload
        :param payload:
        :param dry:
        :return: returns the pysta object representing the ST entity added to the server
        """

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
    """
    BiqQuery STAO.

    use to extract data from a BiqQuery (BQ) dataset.

    subclasses must define
    _fields: list    Explicit list of column names
    _dataset: str    The name of the BQ dataset
    _tablename: str  The name of the BQ table
    """

    _fields = None
    _dataset = None
    _tablename = None

    _where = None
    _orderby = None

    def _extract(self, request):
        print('request {} {}'.format(request, request.json if request else 'no json'))
        try:
            where = request.json.get('where')
        except (ValueError, AttributeError) as e:
            print('error a {}'.format(e))
            where = None

        if self._where:
            where = self._where

        if not where:
            try:
                obj = int(request.json.get(self._cursor_id))
                where = f"{self._cursor_id}>{obj}"
            except (ValueError, AttributeError, TypeError) as e:
                print('error b {}'.format(e))
                where = None

        try:
            self._limit = int(request.json.get('limit'))
        except (ValueError, AttributeError, TypeError):
            pass

        print('where {} {}'.format(where, self._limit))
        return self._handle_extract(self._get_bq_items(self._fields, self._dataset, self._tablename, where=where))

    def _bq_query(self, sql, **kw):
        client = bigquery.Client()
        print(f'BQ Query {sql}')
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
    """
    Location Geoconnex mixin. used to add a "geoconnex" keyword to the Location's properties.

    Because the geoconnex uri is constructed using the Location's @iot.id the entity must be added first to the
    server. The entity's properties are then "patched"
    """

    def _load_record(self, payload, *args, **kw):
        obj = super(LocationGeoconnexMixin, self)._load_record(payload, *args, **kw)
        iotid = obj.iotid
        props = payload['properties']
        props['geoconnex'] = f'https://geoconnex.us/nmwdi/st/locations/{iotid}'

        self._client.patch_location(iotid, payload)


class BucketSTAO(BaseSTAO):
    """
    A STAO for ETLing data from a Google Cloud Storage bucket.

    This is typically only used as a one-shot STAO.  upload large files to the GCS bucket, use a BucketSTAO to ETL to ST

    subclasses must define _blob.  e.g ose_rt_locations.geojson
    """
    _bucket = 'waterdatainitiative'
    _blob = None

    def _get_bucket(self):
        """
        helper function to grab a bucket from GCS
        :return:
        """
        client = storage.Client()
        bucket = client.get_bucket(self._bucket)
        return bucket

    def _extract(self, request):
        """

        :param request: Request object passed in by the CloudFunction trigger
        :return: JSON object
        """

        print(f'extracting bucket {self._bucket}')
        bucket = self._get_bucket()
        blob = bucket.get_blob(self._blob)
        jobj = json.loads(blob.download_as_bytes())
        return self._handle_extract(jobj)

    def _handle_extract(self, jobj):
        """
        :param jobj:
        :return: JSON object
        """

        return jobj
# ============= EOF =============================================
