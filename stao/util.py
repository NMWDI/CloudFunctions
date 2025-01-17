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
"""
util.py This module holds utility functions.

"""



import json
import logging
import random
import datetime

import geojson as geojson
import pyproj
import pytz
from sta.client import Client


def observation_exists(obs, t, v):
    for e in obs:
        tt = make_statime(e['phenomenonTime'])
        tt.replace(tzinfo=pytz.UTC)
        if tt == t and v == e['result']:
            return True


def copy_properties(props, record, attrs):
    for a in attrs:
        props[a] = record[a]


# from sta.sta_client import STAClient
def asiotid(rec):
    return {'@iot.id': rec['@iot.id']}


def make_gwl_payload(stac, rs, tag, last_obs, additional=None):
    if last_obs:
        rs = [ri for ri in rs if stac.make_st_time(ri['DateTimeMeasured']) > last_obs]

    if rs:
        if not additional:
            def additional(x):
                return tuple()

        payload = [(stac.make_st_time(r['DateTimeMeasured']),
                    stac.make_st_time(r['DateTimeMeasured']),
                    f'{float(r[tag]):0.2f}' if r[tag] is not None else None) + additional(r) for r in rs]

        return payload


def gen_continuous_records(stac, cursor, fields, dataset, table_name):
    pointids = get_pointids(cursor, dataset, table_name)
    for location in stac.get_locations(fs="properties/agency eq 'NMBGMR'"):
        name = location['name']
        iotid = location['@iot.id']
        logging.info(f'examining {name} id={iotid}')

        if name not in pointids:
            continue

        records = make_location_gwl(location['name'], cursor, fields, dataset, table_name)
        if not records:
            continue
        logging.info(f'found nrecords={len(records)}')

        rs = [dict(zip(fields, record)) for record in records]
        record = rs[0]

        thing_id = stac.get_thing_id(name='Water Well', location_id=location['@iot.id'])
        if thing_id:
            yield rs, record, thing_id


def get_pointids(cursor, dataset, table_name):
    cursor.flush_results()
    sql = f'''select PointID from {dataset}.{table_name} group by PointID'''

    cursor.execute(sql)
    ret = cursor.fetchall()
    cursor.flush_results()
    return [r[0] for r in ret]


def make_location_gwl(pointid, cursor, fields, dataset, table_name):
    cursor.flush_results()
    fs = ','.join(fields)
    sql = f'''select {fs} from {dataset}.{table_name} where PointID=%(pointid)s'''

    cursor.execute(sql, {'pointid': pointid})
    ret = cursor.fetchall()
    cursor.flush_results()
    return ret


def make_gwl(client, fields, dataset, table_name, previous_max_objectid):
    fs = ','.join(fields)
    sql = f'''select {fs} from {dataset}.{table_name}'''

    logging.info(f'previous max objectid={previous_max_objectid}')
    params = {}
    if previous_max_objectid:
        sql = f'{sql} where OBJECTID>%(leftbounds)s'
        params['leftbounds'] = previous_max_objectid

    total_records = make_total_records(client, dataset, table_name, previous_max_objectid)
    # limit = int(Variable.get('nmbgmr_s_limit', 100))
    limit = 50000

    sql = f'{sql} order by PointID,ObjectID LIMIT {limit}'

    logging.info(f'sql: {sql}')
    qj = client.query(sql, params)

    logging.info(f'total records={total_records}, limit={limit}')
    # data = client.fetchall()
    if limit > total_records:
        logging.info('doing a complete overwrite')

    records = qj.result()

    max_objectid = 0
    if records:
        max_objectid = max((r[fields.index('OBJECTID')] for r in records))

    return records, total_records, max_objectid


def make_total_records(client, dataset, table_name, objectid=None):
    sql = f'''SELECT count(*) from {dataset}.{table_name}'''
    params = {}
    if objectid:
        params['leftbounds'] = objectid
        sql = f'{sql} where OBJECTID>%(leftbounds)s'

    qj = client.query(sql, params)
    rows = qj.result()
    cnt = int(rows[0][0])
    # cursor.flush_results()
    return cnt


# def make_stamqtt_client():
#     connection = BaseHook.get_connection('nmbgmr_sta_conn_id')
#     from sta.sta_client import STAMQTTClient
#     staclient = STAMQTTClient(connection.host)
#     return staclient

def make_statime(t):
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.000Z'):
        try:
            st = datetime.datetime.strptime(t, fmt)
            st = st.replace(tzinfo=datetime.timezone.utc)
            return st
        except ValueError:
            pass




def make_sta_client(project_id=None, secret_id=None):
    from google.cloud import secretmanager

    if project_id is None:
        # GCP project in which to store secrets in Secret Manager.
        project_id = "95715287188"

    if secret_id is None:
        # ID of the secret to create.
        secret_id = "nmwdi_st2_connection"

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    name = f'projects/{project_id}/secrets/{secret_id}/versions/latest'
    response = client.access_secret_version(request={"name": name})

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.

    payload = response.payload.data.decode("UTF-8")
    connection = json.loads(payload)
    stac = Client(connection['host'],
                  connection['username'],
                  connection['password'])
    return stac


PROJECTIONS = {}


def make_geometry_point_from_utm(e, n, zone=None, ellps=None, srid=None):
    if zone:
        if ellps is None:
            ellps = "WGS84"

        key = f"{zone}_{ellps}"
        if key in PROJECTIONS:
            p = PROJECTIONS[key]
        else:
            p = pyproj.Proj(proj="utm", zone=int(zone), ellps=ellps)
            PROJECTIONS[key] = p
    elif srid:
        # get zone
        if srid in PROJECTIONS:
            p = PROJECTIONS[srid]
            PROJECTIONS[srid] = p
        else:
            # p = pyproj.Proj(proj='utm', zone=int(zone), ellps='WGS84')
            p = pyproj.Proj(f"EPSG:{srid}")

    lon, lat = p(e, n, inverse=True)
    return make_geometry_point_from_latlon(lat, lon)


def make_geometry_point_from_latlon(lat, lon, elevation=None):
    coordinates = [float(lon), float(lat)]
    if elevation:
        coordinates.append(float(elevation))

    return {"type": "Point", "coordinates": coordinates}


def make_fuzzy_geometry_from_latlon(lat, lon):
    from shapely import geometry, affinity
    center = geometry.Point(lon, lat)  # Null Island
    radius = 0.008
    xr = random.random()*radius
    yr = random.random()*radius
    center = affinity.translate(center, xoff=xr, yoff=yr)
    circle = center.buffer(radius)  # Degrees Radius

    return json.loads(geojson.dumps(geometry.mapping(circle)))


# def get_prev(context, task_id):
#     newdate = context['prev_execution_date']
#     logging.info(f'prevdate ={newdate}')
#     ti = TaskInstance(context['task'], newdate)
#     previous_max = ti.xcom_pull(task_ids=task_id, key='return_value', include_prior_dates=True)
#     logging.info(f'prev max {previous_max}')
#     return previous_max
#
# LOCATION_DESCRIPTION = 'Location of well where measurements are made'
# GWL_DATASTREAM = 'Groundwater Levels'
# PRESSURE_GWL_DATASTREAM = 'Groundwater Levels(Pressure)'
# ACOUSTIC_GWL_DATASTREAM = 'Groundwater Levels(Acoustic)'
# GWL_DESCRIPTION = 'Measurement of groundwater depth in a water well, as measured below ground surface'
# CONTINUOUS_GWL_DESCRIPTION = 'Measurement of groundwater depth in a water well, as measured below ground surface. ' \
#                              'For continuous Datastreams with more than one measurement per day the MINIMUM depth to ' \
#                              'water is reported '
# RAW_GWL_DATASTREAM = 'Raw Groundwater Depth'
# RAW_GWL_DESCRIPTION = 'Uncorrected measurement of groundwater depth in a water well, as measured from a reference ' \
#                       'measuring point'
# WH_GWL_DATASTREAM = 'Groundwater Head'
# WH_GWL_DESCRIPTION = 'Measurement of water above the transducer. Not Quality Controlled'
#
# AWH_GWL_DATASTREAM = 'Adjusted Groundwater Head'
# AWH_GWL_DESCRIPTION = 'Measurement of water above the transducer. Quality Controlled'
#
# WATER_WELL = 'Water Well'
# BGS_OBSERVED_PROPERTY = ('Depth to Water Below Ground Surface', 'depth to water below ground surface')
# WH_OBSERVED_PROPERTY = ('Groundwater Head', 'Water pressure measured by transducer')
# AWH_OBSERVED_PROPERTY = ('Adjusted Groundwater Head', 'Water pressure measured by transducer corrected by manual '
#                                                       'measurements')
# RAW_OBSERVED_PROPERTY = ('Raw Depth to Water', 'uncorrected measurement of depth to water from measuring point')
# MANUAL_SENSOR_DESCRIPTION = 'Manual measurement of groundwater depth by steel tape, electronic probe or other'
# PRESSURE_SENSOR_DESCRIPTION = '''Continuous (periodic automated) measurement depth to water in Feet below ground
# surface (converted from pressure reading from depth below ground surface in feet). Not Provisional. Quality
# Controlled'''
# ACOUSTIC_SENSOR_DESCRIPTION = '''Continuous (periodic automated) measurement depth to water in Feet below ground
# surface (converted from acoustic device). Not Provisional. Quality Controlled '''
# MANUAL_SENSOR = ('Manual', 'Manual measurement of groundwater depth by steel tape, electronic probe or other')
# ============= EOF =============================================
