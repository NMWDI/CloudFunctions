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
from stao.stao import BQSTAO, LocationGeoconnexMixin
from stao.util import make_geometry_point_from_utm


class NMBGMR_Site_STAO(BQSTAO):
    _fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
               'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
               'StatusDescription', 'FormationZone']

    _dataset = 'locations'
    _tablename = 'nmbgmrSiteMetaData'


class NMBGMRLocations(NMBGMR_Site_STAO, LocationGeoconnexMixin):
    _entity_tag = 'Locations'

    def _transform(self, request, record):
        properties = {k: record[k] for k in ('Altitude', 'AltDatum', 'WellID', 'PointID')}
        properties['agency'] = 'NMBGMR'
        properties['source_id'] = record['OBJECTID']

        e = record['Easting']
        n = record['Northing']
        z = 13

        payload = {'name': record['PointID'].upper(),
                   'description': 'Location of well where measurements are made',
                   'properties': properties,
                   'location': make_geometry_point_from_utm(e, n, z)
                   }

        return payload


class NMBGMRThings(NMBGMR_Site_STAO):
    _entity_tag = 'Things'

    def _get_screens(self, pointid):
        fields = ['ScreenTop', 'ScreenBottom', 'ScreenDescription']
        tablename = 'nmbgmrWellScreens'

        sql = f'select {fields} from {self._dataset}.{tablename} as ws ' \
              f'join {self._dataset}.{self._tablename} as wd on wd.WellID= ws.WellID ' \
              f'where ws.PointID={pointid}'
        return self._bq_query(sql)

    def _transform(self, request, record):
        screens = self._get_screens(record['PointID'])
        payload = {'name': 'Water Well',
                   'description': 'Well drilled or set into subsurface for the purposes '
                                  'of pumping water or monitoring groundwater',
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




#
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

# ============= EOF =============================================
