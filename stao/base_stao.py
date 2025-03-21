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

from stao.util import make_statime, asiotid, make_sta_client, make_geometry_point_from_latlon
from stao.vocab import vocab_factory


#
# try:
#     from util import make_sta_client, observation_exists, asiotid, make_geometry_point_from_latlon, make_statime
#     from vocab import vocab_factory
# except ImportError:
#     from stao.util import make_sta_client, observation_exists, asiotid, make_geometry_point_from_latlon, make_statime
#     from stao.vocab import vocab_factory


class ObservationMixin:
    """
    Observation mixin class.
    """

    _entity_tag = 'observation'

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

    _location = None
    _thing = None
    _datastream = None

    def _get_load_function_name(self):
        return 'add_observations'

    def _get_location(self, record, location_id=None):
        if location_id is None:
            location_id = record['locationId']
            try:
                location_id = int(location_id)
            except ValueError:
                pass

        q = f"properties/source_id eq '{location_id}' and properties/agency eq '{self._agency}'"
        return self._client.get_location(query=q), location_id

    def _location_grouper(self, records):
        def key(r):
            v = r[self._location_field]
            try:
                return int(v)
            except ValueError:
                return v

        return groupby(sorted(records, key=key), key=key)

    def _handle_extract(self, records):
        """
        group the records by the location primary key

        yield a dictionary of the following keys, "locationId", "observations", self._cursor_id

        e.g. {"locationId": 151, "observations": [...], "_airbyte_extracted_at": 7eff-... }

        :param records:
        :return:
        """

        maxo = None
        records = [r for r in records if r[self._value_field] is not None]
        for g, obs in self._location_grouper(records):
            obs = list(obs)
            t = self._get_max_cursor(obs)
            if maxo:
                maxo = max(maxo, t)
            else:
                maxo = t

            yield {'locationId': g, 'observations': obs,
                   self._cursor_id: maxo}

    def _get_max_cursor(self, obs):
        cid = self._cursor_id
        if '.' in cid:
            cid = cid.split('.')[-1]

        return max((o[cid] for o in obs))

    def _transform_value(self, v, record):
        return v

    def _transform_timestamp(self, dt):
        dt = datetime.datetime.utcfromtimestamp(dt)
        dt = dt.replace(tzinfo=pytz.UTC)
        return dt

    def _extract_timestamp(self, dt):
        return int(dt / 1000)

    def _get_thing_name(self, record):
        return self._thing_name

    def _get_thing(self,record, loc):
        name = self._get_thing_name(record)
        return self._client.get_thing(name=name, location=loc['@iot.id'])

    def _get_datastream(self, request, record):
        print(record)
        loc, locationId = self._get_location(record)
        if not loc:
            print(f'******* no location {locationId}')
        else:
            self._location = loc
            try:
                thing = self._get_thing(record, loc)
                self._thing = thing
            except StopIteration:
                print(f'********* no thing for location {locationId}, thing={self._thing_name}, location={loc}')
                return

            if not thing:
                print(f'********* no thing for location {locationId}, thing={self._thing_name}, location={loc}')

            else:
                try:
                    return self._client.get_datastream(name=self._datastream_name, thing=thing['@iot.id'])
                except StopIteration:
                    print(f'********* no datastream for location {locationId}, datastream={self._datastream_name}, '
                          f'thing={thing}')
                    return

    def _get_timestamp(self, obs):
        return obs[self._timestamp_field]

    def _transform(self, request, record):
        ds = self._get_datastream(request, record)
        print('asdfasdfasdfasfas', ds)
        if ds:
            self._datastream = ds
            eobs = self._client.get_observations(ds,
                                                 # limit=2000,
                                                 # pages=1,
                                                 verbose=False,
                                                 orderby='phenomenonTime desc')
            eobs = list(eobs)

            print(f'existing obs={len(eobs)} datastream={ds} ')

            def func(e):
                tt = make_statime(e['phenomenonTime'])
                # tt.replace(tzinfo=pytz.UTC)
                return tt, e['result']

            eeobs = [func(e) for e in eobs]

            vs = []
            duplicates = []
            components = ['phenomenonTime', 'resultTime', 'result']

            for obs in record['observations']:
                # print(obs)

                dt = self._get_timestamp(obs)
                dt = self._extract_timestamp(dt)
                if not dt:
                    print(f'skipping invalid datetime. {dt}')
                    continue
                dt = self._transform_timestamp(dt)

                if not dt:
                    print(f'skipping invalid datetime. {dt}')
                    continue

                # if not last_obs or (last_obs and dt > last_obs):
                t = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                v = obs[self._value_field]
                try:
                    v = float(v)
                    v = self._transform_value(v, obs)
                except (TypeError, ValueError) as e:
                    print(f'skipping. error={e}. v={v}')

                # if self._client.get_observation(t, v):
                #     print(f'skipping already exists {t}, {v}')
                #     continue
                # if observation_exists(eobs, dt, v):
                ee = any(e[0] == dt and e[1] == v for e in eeobs)
                if ee:
                    duplicates.append((t, v))
                    continue
                else:
                    vs.append((t, t, v))
                # print('checking existing obs', len(ee), dt)
                # for (dti, vi) in ee:
                #     # print(vi, v)
                #     # if v == vi:
                #     #     if abs(dt-dti) < datetime.timedelta(days=1):
                #     #         print(dti, dt, dti-dt)
                #
                #     if dti == dt and v == vi:
                #         duplicates.append((t, v))
                #         # print(f'assuming already exists {t}, {v}')
                #         break
                # else:
                #     vs.append((t, t, v))

                # if (dt, v) in eeobs:
                #     duplicates.append((t, v))
                #     # print(f'assuming already exists {t}, {v}')
                #     continue

            if duplicates:
                print(f'found {len(duplicates)} duplicates')
                print(duplicates)

            if vs:
                payload = {'Datastream': asiotid(ds),
                           'observations': vs,
                           'components': components}
                if len(vs)< 100:
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
            if isinstance(request, dict):
                self.state = request
            elif request.json:
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
        records = list(records)
        for i, record in enumerate(records):
            print(f'transform record {i} {self._transform_message(record)}')
            payloads = self._transform(request, record)
            # print('payloads', payloads)
            if payloads:
                if not isinstance(payloads, (tuple, list)):
                    payloads = (payloads,)

                for payload in payloads:
                    # print('--- loading')
                    # print(payload)
                    # print('-------------')
                    self._load_record(payload, dry)
                    cnt += 1
            else:
                print(f'        skipping {record}')
            print('-----------------------------------------------')

            # state = {self._cursor_id: record.get(self._cursor_id),
        state = {self._cursor_id: self._get_latest_cursor(records),
                 'limit': self._limit,
                 'counter': counter + 1
                 }

        print('new state', state)
        self.state = state
        # self.state['counter'] = counter + 1
        return self.state

    def _get_cursor(self, record):
        return record.get(self._cursor_id)

    def _get_latest_cursor(self, records):
        cursors = [self._get_cursor(r) for r in records]
        cursors = [c for c in cursors if c]
        if cursors:
            return max(cursors)

    def _load_record(self, payload, dry):
        """
        Uses the pysta.Client to POST a payload
        :param payload:
        :param dry:
        :return: returns the pysta object representing the ST entity added to the server
        """
        # print('loading record', payload, dry)
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

    def _get_elevation(self, record):
        return


class BQSTAO(BaseSTAO):
    """
    BiqQuery STAO.

    use to extract data from a BiqQuery (BQ) dataset.
    _dataset: str    The name of the BQ dataset

    subclasses must define
    _fields: list    Explicit list of column names
    _tablename: str  The name of the BQ table
    """

    _dataset = 'nmwdi'

    _fields = None
    _tablename = None
    _table_name_alias = None

    _where = None
    _orderby = None
    _join = None

    def _extract(self, request):
        state = None
        if isinstance(request, dict):
            state = request
        elif request:
            state = request.json

        print('request {} {}'.format(request, state if request else 'no json'))
        try:
            where = state.get('where')
        except (ValueError, AttributeError) as e:
            print('error a {}'.format(e))
            where = None

        if not where:
            try:
                if self._cursor_id in ('OBJECTID', 'id'):
                    obj = int(state.get(self._cursor_id))
                    where = f"{self._cursor_id}>{obj}"
                elif self._cursor_id.endswith('_airbyte_raw_id'):
                    obj = state.get(self._cursor_id)
                    where = f"{self._cursor_id}>'{obj}'"
                else:
                    obj = state.get(self._cursor_id)
                    if obj is not None:
                        if self._cursor_id == 'data_time':
                            fmt = '%Y-%m-%d %H:%M:%S'
                            where = f"PARSE_TIMESTAMP('{fmt}', {self._cursor_id})>PARSE_TIMESTAMP('{fmt}', '{obj}')"
                        else:
                            if isinstance(obj, str):
                                try:
                                    fmt = '%Y-%m-%dT%H:%M:%S.%f%z'
                                    dt = datetime.datetime.strptime(obj, fmt)

                                    fmt = '%Y-%m-%d %H:%M:%S'
                                    obj = dt.strftime(fmt)
                                    where = f"{self._cursor_id}>=PARSE_TIMESTAMP('{fmt}', '{obj}')"
                                    print('using where clause 1', where)
                                except ValueError:
                                    for fmt in ('%a, %d %b %Y %H:%M:%S. %Z',
                                                '%Y-%m-%dT%H:%M:%E6S%Ez',
                                                '%Y-%m-%d %H:%M:%S'):
                                        try:
                                            _ = datetime.datetime.strptime(obj, fmt)
                                            where = f"{self._cursor_id}>=PARSE_TIMESTAMP('{fmt}', '{obj}')"
                                            print('using where clause 2', where)
                                            break
                                        except ValueError as e:
                                            print('valueaesfd', e, self._cursor_id, obj)
                                            pass

                                #Fri, 14 Jun 2024 01:04:51 GMT
                                #%a, %d %b %Y %H:%M:%S %Z
                                # where = f"{self._cursor_id}>PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S. %Z', '{obj}')"
                                # where = f"{self._cursor_id}>{obj}"

                            else:
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
            self._limit = int(state.get('limit'))
        except (ValueError, AttributeError, TypeError):
            pass

        join = None
        if self._join:
            join = f'join {self._join}'

        print('where {} {}'.format(where, self._limit))
        return self._handle_extract(self._get_bq_items(self._fields, self._dataset, self._tablename,
                                                       where=where, join=join, table_name_alias=self._table_name_alias))

    def _bq_query(self, sql, **kw):
        client = bigquery.Client(
            project='waterdatainitiative-271000',
        )
        print(f'BQ Query {sql}')
        job = client.query(sql, **kw)
        return job.result()

    def _get_bq_items(self, fields, dataset, tablename, where=None, join=None, table_name_alias=None):
        fs = ','.join(fields)
        sql = f'select {fs} from {dataset}.{tablename}'

        if table_name_alias:
            sql = f'{sql} as {table_name_alias}'

        if join:
            sql = f'{sql} {join} '
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
    _entity_tag = 'datastream'

    def _make_datastream_payload(self, record, tag, agency, thing=None):
        if thing is None:
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
        print('get thing', record)
        name = self.toST('location.name', record)
        q = f"name eq '{name}' and properties/agency eq '{agency}'"
        print(q)
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {name}')
            return

        return self._client.get_thing(location=loc['@iot.id'],
                                      name=self.toST('thing.name'))


class ThingMixin:
    _entity_tag = 'thing'

    def _transform(self, request, record):
        payload = self._make_thing_payload(record)
        return payload

    def _get_location(self, record):
        name = self.toST('location.name', record)
        location = self._client.get_location(f"name eq '{name}'")
        return location

    def _make_thing_payload(self, record):
        location = self._get_location(record)
        payload = {}
        if location:
            properties = self._make_thing_properties(record)
            properties['agency'] = self._agency

            payload = {'name': self.toST('thing.name', record),
                       'Locations': [{'@iot.id': location['@iot.id']}],
                       'description': self.toST('thing.description', record),
                          'properties': properties
                          }
        return payload

    def _make_thing_properties(self, record):
        raise NotImplementedError


class LocationMixin:
    _entity_tag = 'location'

    def _transform(self, request, record):
        payload = self._make_location_payload(record)
        return payload

    def _make_location_payload(self, record):
        lat = self.toST('location.latitude', record)
        lon = self.toST('location.longitude', record)

        elevation = self._get_elevation(record)

        properties = self._make_location_properties(record)
        properties['agency'] = self._agency

        payload = {'name': self.toST('location.name', record),
                   'description': self.toST('location.description', record),
                   'location': make_geometry_point_from_latlon(lat, lon, elevation),
                   "encodingType": "application/vnd.geo+json",
                   "properties": properties
                   }

        return payload

    def _make_location_properties(self, record):
        return {}

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
        try:
            self._client.patch_location(iotid, payload)
        except TypeError:
            print('failed patching location')
            print('payload', payload)


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


class MultifileBucketSTAO(BaseSTAO):
    _bucket_name = 'waterdatainitiative'

    def render(self, request, dry=False):
        """

        :param request: Request object passed in by the CloudFunction trigger
        :param dry: optional keyword for testing.  dry=False goes through the motions but does not send POSTs to the
        ST server
        :return: dict.  return the STAOs state
        """

        if request:
            if isinstance(request, dict):
                self.state = request
            elif request.json:
                self.state = request.json

        for data in self._get_extracted_data():
            self._load(request, data, dry)

        return self.state

    def _get_bucket(self):
        """
        helper function to grab a bucket from GCS
        :return:
        """
        if not self._bucket_name:
            raise NotImplementedError

        client = storage.Client()
        return client.get_bucket(self._bucket_name)

    def _get_extracted_data(self):
        bucket = self._get_bucket()
        blobs = bucket.list_blobs()
        for blob in blobs:
            print(f'extracting {blob.name}')
            yield self._handle_extract(blob.download_as_bytes())

    def _handle_extract(self, blobcontent):
        """
        :param jobj:
        :return: JSON object
        """

        return blobcontent
# ============= EOF =============================================
