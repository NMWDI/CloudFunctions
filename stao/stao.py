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
import datetime
import json
from itertools import groupby
import pytz

from google.cloud import bigquery, storage

try:
    from util import make_sta_client, observation_exists, asiotid, make_geometry_point_from_latlon
    from vocab import vocab_factory
except ImportError:
    from stao.util import make_sta_client, observation_exists, asiotid, make_geometry_point_from_latlon
    from stao.vocab import vocab_factory


class ObservationMixin:
    """
    Observation mixin class.
    """

    _entity_tag = 'observation'

    _value_converter = None

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #  Must define the following attributes
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    _thing_name = None
    _datastream_name = None
    _agency = None
    _location_field = None
    _timestamp_field = None
    _value_field = None
    _cursor_id = None

    def _get_load_function_name(self):
        return 'add_observations'

    def _get_location(self, locationId, record):
        q = f"properties/source_id eq '{locationId}' and properties/agency eq '{self._agency}'"
        return self._client.get_location(query=q)

    def _location_grouper(self, records):
        def key(r):
            return int(r[self._location_field])

        return groupby(sorted(records, key=key), key=key)

    def _handle_extract(self, records):
        """
        group the records by the location primary key

        yield a dictionary of the following keys, "locationId", "observations", self._cursor_id

        e.g. {"locationId": 151, "observations": [...], "_airbyte_ab_id": 7eff-... }

        :param records:
        :return:
        """

        maxo = None
        records = [r for r in records if r[self._value_field] is not None]
        for g, obs in self._location_grouper(records):
            obs = list(obs)
            t = max((o[self._cursor_id] for o in obs))
            if maxo:
                maxo = max(maxo, t)
            else:
                maxo = t

            yield {'locationId': g, 'observations': obs,
                   self._cursor_id: maxo}

    def _extract_timestamp(self, dt):
        return dt/1000

    def _transform(self, request, record):

        locationId = record['locationId']
        locationId = int(locationId)
        loc = self._get_location(locationId, record)
        if not loc:
            print(f'******* no location {locationId}')
        else:
            thing = self._client.get_thing(name=self._thing_name, location=loc['@iot.id'])
            if thing:
                try:
                    ds = self._client.get_datastream(name=self._datastream_name, thing=thing['@iot.id'])
                except StopIteration:
                    return

                if ds:
                    # get last observation for this datastream
                    eobs = self._client.get_observations(ds,
                                                         # limit=1,
                                                         # pages=1,
                                                         verbose=False,
                                                         orderby='phenomenonTime desc')
                    # last_obs = None
                    eobs = list(eobs)
                    # if eobs:
                    #     last_obs = make_statime(eobs[0]['phenomenonTime'])
                    #     last_obs = last_obs.replace(tzinfo=utc)

                    print(f'existing obs={len(eobs)} datastream={ds} ')
                    vs = []
                    components = ['phenomenonTime', 'resultTime', 'result']
                    for obs in record['observations']:
                        dt = obs[self._timestamp_field]
                        dt = self._extract_timestamp(dt)
                        if not dt:
                            print(f'skipping invalid datetime. {dt}')
                            continue
                        dt = datetime.datetime.utcfromtimestamp(dt)
                        dt = dt.replace(tzinfo=pytz.UTC)

                        # if not last_obs or (last_obs and dt > last_obs):
                        t = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        v = obs[self._value_field]
                        try:
                            v = float(v)
                            if self._value_converter:
                                v = self._value_converter(v)

                        except (TypeError, ValueError) as e:
                            print(f'skipping. error={e}. v={v}')

                        if observation_exists(eobs, dt, v):
                            print(f'skipping already exists {t}, {v}')
                            continue
                        vs.append((t, t, v))
                    print(vs)
                    if vs:
                        payload = {'Datastream': asiotid(ds),
                                   'observations': vs,
                                   'components': components}
                        print('------------- payload', payload)
                        return payload


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
    _vocab_tag = None

    def __init__(self, secret_id=None, project_id=None):
        """
        """
        self._client = make_sta_client(project_id=project_id, secret_id=secret_id)
        self.state = {}
        self._vocab_mapper = vocab_factory(self._vocab_tag)

    def toST(self, *args, **kw):
        return self._vocab_mapper.toST(*args, **kw)

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
        self.state['counter'] = counter + 1
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

        if not where:
            try:
                if self._cursor_id == 'OBJECTID':
                    obj = int(request.json.get(self._cursor_id))
                    where = f"{self._cursor_id}>{obj}"
                else:
                    obj = request.json.get(self._cursor_id)
                    if obj is not None:
                        where = f"{self._cursor_id}>'{obj}'"
            except (ValueError, AttributeError, TypeError) as e:
                print('error b {}'.format(e))
                where = None

        if self._where:
            if where:
                where = f'{where} and {self._where}'
            else:
                where = self._where

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


class DatastreamMixin:
    def _make_datastream_payload(self, record, tag, agency):
        thing = self._get_thing(record, agency)
        if thing:
            dtw = self.toST(f'{tag}.observed_property.name')
            sn = self.toST(f'{tag}.sensor.name')
            obsprop = next(self._client.get_observed_properties(name=dtw))
            sensor = next(self._client.get_sensors(name=sn))

            dtwbgs = {'name': self.toST(f'{tag}.datastream.name'),
                      'description': self.toST(f'{tag}.datastream.description'),
                      'Sensor': asiotid(sensor),
                      'ObservedProperty': asiotid(obsprop),
                      'Thing': asiotid(thing),
                      'unitOfMeasurement': self.toST(f'{tag}.unitOfMeasurement'),
                      'observationType': self.toST(f'{tag}.observationType')
                      }
            return dtwbgs

    def _get_thing(self, record, agency):
        name = self.toST('location.name', record)
        q = f"name eq '{name}' and properties/agency eq '{agency}'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {name}')
            return

        return self._client.get_thing(location=loc['@iot.id'],
                                       name=self.toST('thing.name'))


class ThingMixin:
    def _make_thing_payload(self, record):
        name = self.toST('location.name', record)
        location = self._client.get_location(f"name eq '{name}'")
        payload = {}
        if location:
            payload = {'name': self.toST('thing.name', record),
                       'Locations': [{'@iot.id': location['@iot.id']}],
                       'description': self.toST('thing.description', record),
                       }
        return payload


class LocationMixin:
    def _make_location_payload(self, record):
        lat = self.toST('location.latitude', record)
        lon = self.toST('location.longitude', record)

        payload = {'name': self.toST('location.name', record),
                   'description': self.toST('location.description', record),
                   'location': make_geometry_point_from_latlon(lat, lon),
                   "encodingType": "application/vnd.geo+json",
                   }
        return payload


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
