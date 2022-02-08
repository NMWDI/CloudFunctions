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

import geojson
from google.cloud import storage
from jsonschema import validate
from sta.definitions import OM_Measurement, FOOT, GPM

try:
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
        LOCATION_DESCRIPTION, asiotid
except ImportError:
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, BucketSTAO
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, \
        LOCATION_DESCRIPTION, asiotid


def location_name(record):
    sid = record['Station_ID']
    if sid:
        return f'{sid:04n}'

    # gn = record['Gauge_name']
    # if sid and gn:
    #     if sid:
    #         sid = f'{sid:04n}-'
    #     else:
    #         sid = ''
    #
    #     return f"{sid}{gn}"


class OSERealtime_STAO(BucketSTAO):
    _blob = 'ose_rt_locations.geojson'

    def _make_location(self, record):
        record = record['properties']
        name = location_name(record)
        if not name:
            print(f'Invalid name. record={record}')
            return

        if not record['lat_ddd'] or not record['long_ddd']:
            print(f'Invalid lat_ddd/long_ddd. record={record}')
            return

        try:
            location = self._client.get_location(f"name eq '{name}'")
        except BaseException as e:
            print(f'failed getting location for {name}. exception={e}')
            return

        return record, location

    def _make_properties(self, record, attrs):
        properties = {k: record[k] for k in attrs}
        properties['agency'] = 'OSE'
        properties['source_id'] = record['OBJECTID']
        return properties

    def _handle_extract(self, jobj):
        return jobj['features']


class OSERealtimeLocations(OSERealtime_STAO, LocationGeoconnexMixin):
    _entity_tag = 'location'

    def _transform(self, request, record):
        '''
        "properties": {"OBJECTID": 63, "Op_Initial": "jt", "OSE_File": "los indos", "POD_nbr": None,
                       "Ditch_Name": "de los Indios", "River_src": "P", "Field_ID": "pojoaque",
                       "Photo_ID": "<Null>", "Comments": "Indios", "Northing": 3972566.3466362511,
                       "Easting": 402515.03543825593, "Edit_Date": "10\/7\/09", "Photos": " ", "Weblink": None,
                       "Gauge_name": "Indios", "Basin": " ", "Number_": None, "Suffix": "flume",
                       "Meter_type": "Radio", "Meter_status": "Complete", "Percent_progress": 100,
                       "Jurisdiction": "OSE", "Latitude": "35° 53' 33.076\" N",
                       "Longitude": "106° 4' 48.535\" W", "lat_ddd": 35.892521186099707,
                       "long_ddd": -106.08014852530609, "SW_or_GW": "S", "Station_ID": 23.0,
                       "recent_discharge": 2.24,
                       "data_url": "http:\/\/meas.ose.state.nm.us\/site.jsp?id=23&status=Y&type=S"},

        :param request:
        :param record:
        :return:
        '''

        record = record['properties']
        name = location_name(record)
        if not name:
            print(f'Invalid name.  record={record}')
            return

        lat = record['lat_ddd']
        lon = record['long_ddd']
        if not lat or not lon:
            print(f'Invalid lat={lat} or lon={lon}')
            return

        properties = self._make_properties(record, ('Ditch_Name', 'River_src', 'Field_ID', 'Op_Initial', 'OSE_File',
                                                    'POD_nbr', 'Comments', 'Basin', 'Station_ID', 'Gauge_name',
                                                    'Number_',
                                                    'Suffix', 'Jurisdiction', 'data_url',
                                                    'Photo_ID', 'Comments', 'Edit_Date', 'Photos', 'SW_or_GW'))

        payload = {'name': name,
                   'description': 'Location of station where real time measurements are made',
                   'properties': properties,
                   'location': make_fuzzy_geometry_from_latlon(lat, lon),
                   "encodingType": "application/vnd.geo+json",
                   }
        return payload


class OSERealtimeThings(OSERealtime_STAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        args = self._make_location(record)
        if args:
            record, location = args
            properties = self._make_properties(record, ('Comments', 'Station_ID', 'Gauge_name', 'data_url', 'Comments',
                                                        'SW_or_GW', 'Meter_type', 'Meter_status'))

            payload = {'name': 'OSE Realtime Station',
                       'description': 'OSE Realtime Station',
                       'Locations': [asiotid(location)],
                       'properties': properties}
            return payload


class OSERealtimeDatastreams(OSERealtime_STAO):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        args = self._make_location(record)
        if args:
            record, location = args
            sensor = record['Meter_type']

            sensor = next(self._client.get_sensors(name=sensor))
            dis = next(self._client.get_observed_properties(name='OSERealTimeDischarge'))
            ga = next(self._client.get_observed_properties(name='OSERealTimeGageHeight'))
            thing = self._client.get_thing(name='OSE Realtime Station', location=location['@iot.id'])

            dis = asiotid(dis)
            ga = asiotid(ga)
            sensor = asiotid(sensor)
            thing = asiotid(thing)

            payloads = [{'name': "OSERealTime Discharge",
                         'description': 'No Description',
                         'Sensor': sensor,
                         'ObservedProperty': dis,
                         'Thing': thing,
                         'unitOfMeasurement': GPM,
                         'observationType': OM_Measurement,
                         },
                        {'name': "OSERealTime Gage Height",
                         'description': 'No Description',
                         'Sensor': sensor,
                         'ObservedProperty': ga,
                         'Thing': thing,
                         'unitOfMeasurement': FOOT,
                         'observationType': OM_Measurement,
                         }
                        ]

            return payloads


class OSERealtimeSensors(OSERealtime_STAO):
    _entity_tag = 'sensor'

    def _transform(self, request, record):
        args = self._make_location(record)
        if args:
            record, location = args
            payload = {'name': record['Meter_type'],
                       'description': 'No Description',
                       'encodingType': 'application/pdf',
                       'metadata': 'No Metadata'}

            return payload


class OSERealtimeObservedProperties(OSERealtime_STAO):
    _entity_tag = 'observed_property'

    def _transform(self, request, record):
        args = self._make_location(record)
        if args:
            payloads = [{'name': 'OSERealTimeDischarge',
                         'description': 'Discharge (gpm)',
                         'definition': 'No definition'},
                        {'name': 'OSERealTimeGageHeight',
                         'description': 'Gage Height (ft bgs)',
                         'definition': 'No definition'}]
            return payloads


if __name__ == '__main__':
    # stao = OSERealtimeLocations()
    # stao.render(None, dry=False)

    # stao = OSERealtimeThings()
    stao = OSERealtimeDatastreams()
    stao.render(None, dry=False)

# ============= EOF =============================================
