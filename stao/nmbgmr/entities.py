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

import jsonschema
import requests
from sta.definitions import FOOT, OM_Measurement
from sta.util import statime

try:
    from stao import BQSTAO, LocationGeoconnexMixin, ObservationMixin
    from util import make_geometry_point_from_utm, asiotid, make_statime
    from constants import GWL_DS, DTW_OBS_PROP, MANUAL_SENSOR, PRESSURE_SENSOR, WATER_QUANTITY, ACOUSTIC_SENSOR, \
        WELL_LOCATION_DESCRIPTION, WATER_WELL
except ImportError:
    from stao.stao import BQSTAO, LocationGeoconnexMixin, ObservationMixin
    from stao.util import make_geometry_point_from_utm, asiotid, make_statime
    from stao.constants import GWL_DS, DTW_OBS_PROP, MANUAL_SENSOR, PRESSURE_SENSOR, WATER_QUANTITY, ACOUSTIC_SENSOR, \
        WELL_LOCATION_DESCRIPTION, WATER_WELL


NMBGMR = 'NMBGMR'

class NMBGMR_Site_STAO(BQSTAO):
    _fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
               'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
               'StatusDescription', 'FormationZone']

    _dataset = 'locations'
    _tablename = 'nmbgmr_sites'

    _limit = 100
    _orderby = 'OBJECTID asc'

    def _transform_message(self, record):
        return f"OBJECTID={record['OBJECTID']}"


LOCATION_SCHEMA = {
    "$id": "https://vocab.newmexicowaterdata.org/schemas/location",
    "title": "NMWDI Location Schema",
    "description": "",
    "version": "0.0.1",
    "type": "object",
    "required": ["name", "description", "properties"],
    "properties": {
        "name": {
            "type": "string",
            "description": "unique human readable identifier, e.g PointID",
            "fields": {"NMBGMR": "PointID"}

        },
        "description": {
            "type": "string",
            "description": "description of this location"
        },
        "properties": {
            "type": "object",
            "description": "a flexible place to associate additional attributes with a location",
            "required": ["elevation", "elevation_unit", "elevation_datum", "dataprovider"],
            "properties": {"dataprovider": {"type": "string",
                                            "description": "agency that measured this data",
                                            "fields": {"NMBGMR": "<NMBGMR>"},
                                            "enum": ["OSE", "CABQ", "EBID", "PVACD", "NMBGMR", "OSE-Roswell",
                                                     "ISC_SEVEN_RIVERS"]},
                           "elevation": {"type": "number",
                                         "fields": {"NMBGMR": "Altitude"}},
                           "elevation_unit": {"type": "string",
                                              "enum": ["MASL", "FASL"],
                                              "fields": {"NMBGMR": "<FASL>"}},
                           "elevation_datum": {"type": "string",
                                               "fields": {"NMBGMR": "AltDatum"}},
                           }
        }
    }
}


class NMBGMRLocations(LocationGeoconnexMixin, NMBGMR_Site_STAO):
    _entity_tag = 'location'

    def _get_location_schema(self):
        global LOCATION_SCHEMA
        if LOCATION_SCHEMA is None:
            url = 'https://raw.githubusercontent.com/NMWDI/VocabService/main/schemas/location.schema.json#'
            resp = requests.get(url)
            LOCATION_SCHEMA = resp.json()
        return LOCATION_SCHEMA

    def _assemble_properties(self, record):
        schema = self._get_location_schema()
        sprops = schema['properties']['properties']['properties']

        properties = {}
        for k, prop in sprops.items():
            field = prop['fields'][NMBGMR]
            if field.startswith('<') and field.endswith('>'):
                properties[k] = field[1:-1]
            else:
                properties[k] = record[field]

            # properties = {k: record[k] for k in ('Altitude', 'AltDatum', 'WellID', 'PointID')}
            # properties['agency'] = 'NMBGMR'
            # properties['source_id'] = record['OBJECTID']

        return properties

    def _assemble_payload(self, record):
        payload = {}
        schema = self._get_location_schema()
        properties = schema['properties']
        namefield = properties['name']['fields'][NMBGMR]
        payload['name'] = record[namefield]

        return payload

    def _transform(self, request, record):
        properties = self._assemble_properties(record)

        e = record['Easting']
        n = record['Northing']
        z = 13
        payload = self._assemble_payload(record)

        payload.update({'name': record['PointID'].upper(),
                        'description': WELL_LOCATION_DESCRIPTION,
                        'properties': properties,
                        'location': make_geometry_point_from_utm(e, n, z),
                        "encodingType": "application/vnd.geo+json",
                        })

        return payload


class NMBGMRThings(NMBGMR_Site_STAO):
    _entity_tag = 'thing'

    def _get_screens(self, pointid):
        fields = ['ScreenTop', 'ScreenBottom', 'ScreenDescription']
        fs = ",".join(fields)
        tablename = 'nmbgmr_well_screens'

        sql = f'select {fs} from {self._dataset}.{tablename} ' \
              f'where PointID="{pointid}"'
        rows = self._bq_query(sql)
        ret = [{fi: row[fi] for fi in fields} for row in rows]

        return ret

    def _transform(self, request, record):
        name = record['PointID']
        location = self._client.get_location(f"name eq '{name}'")
        screens = self._get_screens(name)
        payload = {'name': WATER_WELL['name'],
                   'Locations': [{'@iot.id': location['@iot.id']}],
                   'description': WATER_WELL['description'],
                   'properties': {'WellDepth': record['WellDepth'],
                                  'GeologicFormation': record['FormationZone'],
                                  'Use': record['CurrentUseDescription'],
                                  'Status': record['StatusDescription'],
                                  'Screens': screens,
                                  'agency': 'NMBGMR',
                                  'PointID': record['PointID'],
                                  'WellID': record['WellID'],
                                  'source_id': record['OBJECTID']}
                   }

        return payload


class NMBGMRWaterLevelDatastreams(BQSTAO):
    # _fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
    #            'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
    #            'StatusDescription', 'FormationZone']

    _dataset = 'levels'
    _entity_tag = 'datastream'

    def _get_bq_items(self, fields, dataset, tablename, where=None):
        if 'OBJECTID' not in fields:
            fields.append('OBJECTID')

        fs = ','.join(fields)

        subquery = f'''(select *, row_number() over (PARTITION by PointID ORDER BY OBJECTID asc) rn
        from {dataset}.{tablename}) x'''
        sql = f'select {fs} from {subquery} where x.rn=1 '
        if where:
            sql = f'{sql} and {where}'

        sql = f'{sql} order by OBJECTID asc'

        if self._limit:
            sql = f'{sql} limit {self._limit}'

        return self._bq_query(sql)


class NMBGMRManualWaterLevelsDatastreams(NMBGMRWaterLevelDatastreams):
    _tablename = 'nmbgmr_manual_gwl'
    _fields = ['OBJECTID', 'PointID',
               'MeasuringAgency', 'MeasurementMethod', 'LevelStatus', 'DataSource', 'DataQuality']
    _limit = 500

    def _transform(self, request, record):
        pointid = record['PointID']

        q = f"name eq '{pointid}' and properties/agency eq 'NMBGMR'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {pointid}')
            return

        thing = self._client.get_thing(location=loc['@iot.id'], name='Water Well')
        if thing:
            obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
            sensor = next(self._client.get_sensors(name=MANUAL_SENSOR['name']))
            properties = {}
            dtwbgs = {'name': GWL_DS['name'],
                      'description': GWL_DS['description'],
                      'Sensor': asiotid(sensor),
                      'ObservedProperty': asiotid(obsprop),
                      'Thing': asiotid(thing),
                      'unitOfMeasurement': FOOT,
                      'observationType': OM_Measurement,
                      'properties': properties
                      }
            return dtwbgs
        # payloads = [dtw, dtwbgs]
        # return payloads


class NMBGMRPressureWaterLevelsDatastreams(NMBGMRWaterLevelDatastreams):
    _tablename = 'pressure_gwl'
    _fields = ['OBJECTID', 'PointID',
               'MeasuringAgency', 'MeasurementMethod', 'DataSource', 'DataSource']
    _limit = 500

    def _transform(self, request, record):
        pointid = record['PointID']

        q = f"name eq '{pointid}' and properties/agency eq 'NMBGMR'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {pointid}')
            return

        thing = self._client.get_thing(location=loc['@iot.id'], name='Water Well')
        if thing:
            obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
            sensor = next(self._client.get_sensors(name=PRESSURE_SENSOR['name']))
            properties = {'MeasuringAgency': record['MeasuringAgency'],
                          'DataSource': record['DataSource'],
                          'agency': 'NMBGMR',
                          'topic': WATER_QUANTITY}

            dtwbgs = {'name': GWL_DS['name'],
                      'description': GWL_DS['description'],
                      'Sensor': asiotid(sensor),
                      'ObservedProperty': asiotid(obsprop),
                      'Thing': asiotid(thing),
                      'unitOfMeasurement': FOOT,
                      'observationType': OM_Measurement,
                      'properties': properties
                      }
            return dtwbgs


class NMBGMRAcousticWaterLevelsDatastreams(NMBGMRWaterLevelDatastreams):
    _tablename = 'acoustic_gwl'
    _fields = ['OBJECTID', 'PointID',
               'MeasuringAgency', 'MeasurementMethod', 'DataSource', 'DataSource']
    _limit = 500

    def _transform(self, request, record):
        pointid = record['PointID']

        q = f"name eq '{pointid}' and properties/agency eq 'NMBGMR'"
        loc = self._client.get_location(query=q)
        if not loc:
            print(f'------------ failed locating {pointid}')
            return

        thing = self._client.get_thing(location=loc['@iot.id'], name='Water Well')
        if thing:
            obsprop = next(self._client.get_observed_properties(name=DTW_OBS_PROP['name']))
            sensor = next(self._client.get_sensors(name=ACOUSTIC_SENSOR['name']))
            properties = {'MeasuringAgency': record['MeasuringAgency'],
                          'DataSource': record['DataSource'],
                          'agency': 'NMBGMR',
                          'topic': WATER_QUANTITY}

            dtwbgs = {'name': GWL_DS['name'],
                      'description': GWL_DS['description'],
                      'Sensor': asiotid(sensor),
                      'ObservedProperty': asiotid(obsprop),
                      'Thing': asiotid(thing),
                      'unitOfMeasurement': FOOT,
                      'observationType': OM_Measurement,
                      'properties': properties
                      }
            return dtwbgs


class NMBGMRWaterLevelsObservations(BQSTAO, ObservationMixin):
    _dataset = 'levels'
    _entity_tag = 'observation'

    _fields = ['OBJECTID', 'PointID',
               'MeasuringAgency', 'MeasurementMethod', 'DataSource', 'DataSource',
               'DateTimeMeasured', 'DepthToWaterBGS']
    _limit = 500
    _orderby = 'OBJECTID asc'

    def __init__(self, tablename, *args, **kw):
        super(NMBGMRWaterLevelsObservations, self).__init__(*args, **kw)
        self._tablename = tablename

    def _handle_extract(self, records):
        def key(r):
            return r['PointID']

        maxo = 0
        for g, obs in groupby(sorted(records, key=key), key=key):
            obs = list(obs)
            OBJECTID = max((o['OBJECTID'] for o in obs))

            maxo = max(maxo, OBJECTID)
            yield {'PointID': g, 'observations': obs, 'OBJECTID': maxo}

    def _transform(self, request, record):
        loc = self._client.get_location(name=record['PointID'])
        if not loc:
            print(f'******* no location {record["PointID"]}')
        else:
            thing = self._client.get_thing(name='Water Well', location=loc['@iot.id'])
            if thing:
                try:
                    ds = self._client.get_datastream(name=GWL_DS['name'], thing=thing['@iot.id'])
                except StopIteration:
                    return

                if ds:

                    # get last observation for this datastream
                    eobs = self._client.get_observations(ds, limit=1,
                                                         pages=1,
                                                         verbose=False,
                                                         orderby='phenomenonTime desc')
                    last_obs = None
                    eobs = list(eobs)
                    if eobs:
                        last_obs = make_statime(eobs[0]['phenomenonTime'])
                    print(f'last obs datastream={ds} lastobs={last_obs} ')
                    vs = []
                    components = ['phenomenonTime', 'resultTime', 'result']
                    for obs in record['observations']:
                        dt = obs['DateTimeMeasured']
                        if not dt:
                            print(f'skipping invalid datetime. {dt}')
                            continue

                        if not last_obs or (last_obs and dt > last_obs):
                            # t = statime(dt)
                            # if t is None:
                            #     print(f'skipping invalid datetime. {dt}')
                            #     continue
                            # t = f'{t.isoformat()}.000Z'

                            t = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                            v = obs['DepthToWaterBGS']
                            try:
                                v = float(v)
                                vs.append((t, t, v))
                            except (TypeError, ValueError) as e:
                                print(f'skipping. error={e}. v={v}')

                    if vs:
                        payload = {'Datastream': asiotid(ds),
                                   'observations': vs,
                                   'components': components}
                        print('------------- payload', payload)
                        return payload


# def make_screens(client, objectid, dataset, site_table_name):
#     # dataset = Variable.get('bq_locations')
#     # table_name = Variable.get('nmbgmr_screen_tbl')
#     # site_table_name = Variable.get('nmbgmr_site_tbl')
#     table_name = 'nmbgmrWellScreens'
#
#     # sql = f'select PointID, ScreenTop, ScreenBottom, ScreenDescription from {dataset}.{table_name} ' \
#     #       f'where PointID in (%(pids)s) order by PointID'
#     columns = 'ws.PointID', 'ScreenTop', 'ScreenBottom', 'ScreenDescription'
#     cs = ','.join(columns)
#     # sql = f'select {cs} from {dataset}.{table_name} ' \
#     #       f'order by PointID'
#
#     sql = f'select {cs} from {dataset}.{table_name} as ws ' \
#           f'join {dataset}.{site_table_name} as wd on wd.WellID= ws.WellID ' \
#           f'where wd.OBJECTID>%(OBJECTID)s ' \
#           f'order by ws.PointID'
#
#     # pids = ','.join([f'"{w}"' for w in pids])
#
#     # logging.info(sql)
#     # logging.info(pids)
#     qj = client.query(sql, parameters={'OBJECTID': objectid or 0})
#     records = qj.result()
#
#     screens = {}
#     for wi, s in groupby(records, key=itemgetter(0)):
#         # logging.info(wi)
#         # s = list(s)
#         # logging.info(s)
#         screens[wi] = [{c: si for c, si in zip(columns[1:], ss[1:])} for ss in s]
#     return screens
#
# def etl_locations_things(request):
#     fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
#               'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
#               'StatusDescription', 'FormationZone']
#
#     dataset = 'locations'
#     table_name = 'nmbgmrSiteMetaData'
#
#     fs = ','.join(fields)
#     sql = f'''select {fs} from {dataset}.{table_name}'''
#
#     # previous_max_objectid = get_prev(context, 'nmbgmr-etl')
#     request_json = request.get_json()
#     previous_max_objectid = None
#     if request_json:
#         previous_max_objectid = request_json['objectid']
#
#     if previous_max_objectid:
#         sql = f'{sql} where OBJECTID>%(leftbounds)s'
#
#     limit = 10000
#
#     sql = f'{sql} order by OBJECTID LIMIT {limit}'
#     params = {'leftbounds': previous_max_objectid}
#     return f'Going to execute {sql}, params={params}'
#     # client = bigquery.Client()
#     # # conn = bq.get_conn()
#     # # client = conn.client()
#     # st = time.time()
#     # screens = make_screens(client, previous_max_objectid)
#     # logging.info(f'got screens {len(screens)} {time.time() - st}')
#     #
#     # total_records = make_total_records(client, dataset, table_name)
#     # logging.info(f'total records={total_records}, limit={limit}')
#     # # data = client.fetchall()
#     # if limit > total_records:
#     #     logging.info('doing a complete overwrite')
#     #
#     # stac = make_sta_client()
#     # # client.execute(sql, params)
#     # qj = client.query(sql, params)
#     # fetched_any = False
#     # gst = time.time()
#     # cnt = 0
#     #
#     # for record in qj.result():
#     #
#     # # while 1:
#     #     # record = client.fetchone()
#     #     # if not record:
#     #     #     break
#     #
#     #     fetched_any = True
#     #     record = dict(zip(fields, record))
#     #     logging.info(record)
#     #     properties = {k: record[k] for k in ('Altitude', 'AltDatum')}
#     #     properties['agency'] = 'NMBGMR'
#     #     properties['source_id'] = record['OBJECTID']
#     #     properties['PointID'] = record['PointID']
#     #     properties['WellID'] = record['WellID']
#     #     name = record['PointID'].upper()
#     #     description = 'Location of well where measurements are made'
#     #     e = record['Easting']
#     #     n = record['Northing']
#     #     z = 13
#     #     # logging.info(f'PointID={name}, Easting={e},Northing={n}')
#     #     st = time.time()
#     #     lid, added = stac.put_location(name, description, properties, utm=(e, n, z))
#     #     logging.info(f'added location {lid} {time.time() - st}')
#     #     properties['geoconnex'] = f'https://geoconnex.us/nmwdi/st/locations/{lid}'
#     #
#     #     stac.patch_location(lid, {'properties': properties})
#     #
#     #     name = 'Water Well'
#     #     description = 'Well drilled or set into subsurface for the purposes ' \
#     #                   'of pumping water or monitoring groundwater'
#     #
#     #     properties = {'WellDepth': record['WellDepth'],
#     #                   'GeologicFormation': record['FormationZone'],
#     #                   'Use': record['CurrentUseDescription'],
#     #                   'Status': record['StatusDescription'],
#     #                   'Screens': screens.get(record['PointID'], []),
#     #                   'agency': 'NMBGMR',
#     #                   'PointID': record['PointID'],
#     #                   'WellID': record['WellID'],
#     #                   'source_id': record['OBJECTID']}
#     #     # logging.info(f'Add thing to {lid}')
#     #     st = time.time()
#     #     stac.put_thing(name, description, properties, lid)
#     #     logging.info(f'added thing to {lid} {time.time() - st}')
#     #     previous_max_objectid = record['OBJECTID']
#     #     cnt += 1
#     #
#     # t = time.time() - gst
#     # logging.info(f'total upload time={t} n={cnt} avg={cnt / t}')
#     #
#     # if not fetched_any or cnt >= total_records:
#     #     # start back at begin to reexamine sites
#     #     previous_max_objectid = 0
#     #
#     # return previous_max_objectid

class DummyRequest:
    def __init__(self, p):
        self._p = p

    @property
    def json(self):
        return self._p


if __name__ == '__main__':
    d = {
        "name": '10',
        "description": "faar",
        "bel": 123,
        "properties": {
            "dataprovider": "NMBGMR",
            "elevation": 100,
            "elevation_unit": "FASL",
            "elevation_datum": "Foo"
        }
    }
    schema = {"type": "object",
              "properties": {
                  "name": {"type": "string"},
                  "bel": {"type": "number"},
                  "properties": {"type": "object",
                                 "properties": {"elevation": {"type": "number"}},
                                 "required": ["elevation"]
                                 }}}

    schema = {
        "$id": "https://vocab.newmexicowaterdata.org/schemas/location",
        "title": "NMWDI Location Schema",
        "description": "",
        "vesrion": "0.0.1",
        "type": "object",
        "required": ["name", "description", "properties"],
        "properties": {
            "name": {
                "type": "string",
                "description": "unique human readable identifier, e.g PointID"
            },
            "description": {
                "type": "string",
                "description": "description of this location"
            },
            "properties": {
                "type": "object",
                "description": "a flexible place to associate additional attributes with a location",
                "properties": {
                    "dataprovider": {"type": "string",
                                     "description": "agency that measured this data",
                                     "enum": ["OSE", "CABQ", "EBID", "PVACD", "NMBGMR", "OSE-Roswell",
                                              "ISC_SEVEN_RIVERS"]},
                    "elevation": {"type": "number"},
                    "elevation_unit": {"type": "string",
                                       "enum": ["MASL", "FASL"]}},
                "required": ["elevation", "elevation_unit", "dataprovider"]
            }
        }
    }

    # schema = {
    #     "type": "object",
    #     "properties": {
    #         "price": {"type": "number"},
    #         "name": {"type": "string"},
    #     }
    # }
    # d = {"name" : "Eggs", "price" : "Invalid"}
    jsonschema.validate(instance=d, schema=schema)
    # # c = NMBGMRLocations()
    # # c = NMBGMRAcousticWaterLevelsDatastreams()
    # c = NMBGMRWaterLevelsObservations('pressure_gwl')
    # # c._limit = 5
    # for i in range(2):
    #     if i:
    #         # state = json.loads(ret)
    #         dr = DummyRequest({'where': f"OBJECTID>{state['OBJECTID']}"})
    #     else:
    #         dr = DummyRequest({})
    #     state = c.render(dr)
    #
    # # c = NMBGMRManualWaterLevelsDatastreams()
    # # for i in range(2):
    # #     if i:
    # #         OBJECTID = c.state['OBJECTID']
    # #         rr = {'where': f'OBJECTID>{OBJECTID}'}
    # #     else:
    # #         rr = {}
    # #
    # #     r = DummyRequest(rr)
    # #     c.render(r, dry=True)

# ============= EOF =============================================
