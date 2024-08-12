# ===============================================================================
# Copyright 2022 ross
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
from itertools import groupby

from sta.definitions import FOOT, OM_Measurement

from stao.hydrovu import HydroVuLocations, HydroVuWaterLevelsDatastreams, HydroVuObservations, HydroVuThings

# try:
#     from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin
#     from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
#         asiotid, make_statime
#     from hydrovu import HydroVuLocations, HydroVuThings, HydroVuWaterLevelsDatastreams, HydroVuObservations
# except ImportError as e:
#     print('import error', e)
#     from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
#         DatastreamMixin
#     from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
#         make_fuzzy_geometry_from_latlon, asiotid, make_statime
#     from stao.hydrovu import HydroVuLocations, HydroVuThings, HydroVuWaterLevelsDatastreams, HydroVuObservations

AGENCY = 'PVACD'

# class PHV_Site_STAO(BQSTAO):
#     _vocab_tag = 'phv'
#     _fields = ['id', 'name', 'latitude', 'longitude', 'description']
#
#     _dataset = 'locations'
#     _tablename = 'pecos_locations'
#
#     _limit = 100
#     _orderby = 'id asc'
#     _where = "LOWER(name) like '%level%'"
#
#     def _transform_message(self, record):
#         return f"id={record['id']}, name={record['name']}"


# class PHVLocations(LocationGeoconnexMixin, PHV_Site_STAO, LocationMixin):

METADATA = [
    {
        'pointid': "NM-28258",
        'name': "Zumwalt level",
        'elevation': 3459,
        'welldepth': 950,
        'holedepth': 0,
        'id': 4538855792574464,
    },
    {
        'pointid': "NM-28255",
        'name': "Greenfield level",
        'elevation': 880,
        'welldepth': 880,
        'holedepth': 0,
        'id': 5830701895778304,
    },
    {
        'pointid': "NM-28250",
        'name': "Poe Corn Level",
        'elevation': 3622,
        'welldepth': 435,
        'holedepth': 0,
        'id': 6054555505917952,
    },
    {
        'pointid': "NM-28253",
        'name': "LFD Level ",
        'elevation': 512,
        'welldepth': 512,
        'holedepth': 0,
        'id': 5597309948919808,
    },
    {
        'pointid': "NM-28254",
        'name': "Orchard Park Level",
        'elevation': 3539,
        'welldepth': 930,
        'holedepth': 930,
        'id': 6505900885147648,
    },
    {
        'pointid': "NM-28256",
        'name': "Bartlett level",
        'elevation': 1150,
        'welldepth': 1150,
        'holedepth': 0,
        'id': 4745648669458432,
    },
    {
        'pointid': "NM-28259",
        'name': "Artesia A Level",
        'elevation': 3402,
        'welldepth': 726,
        'holedepth': 1008,
        'id': 6256156690612224,
    },
    {
        'pointid': "NM-28257",
        'name': "Cottonwood level",
        'elevation': 3529,
        'welldepth': 950,
        'holedepth': 0,
        'id': 4803999894339584,
    },
    {
        'pointid': "NM-28251",
        'name': "Transwestern Level",
        'elevation': 3618,
        'welldepth': 352,
        'holedepth': 0,
        'id': 4586726273318912,
    },
    {
        'pointid': "NM-28252",
        'name': "Berrendo-Smith level",
        'elevation': 3581,
        'welldepth': 329,
        'holedepth': 0,
        'id': 4847162637942784,
    },
]


class PHVLocations(HydroVuLocations):
    _vocab_tag = 'phv'
    _tablename = 'pvacd_locations'
    _agency = AGENCY
    _where = "LOWER(name) like '%level%'"

    def _get_elevation(self, record):
        for well in METADATA:
            if well['name'] == record['name']:
                return well['elevation'] / 3.28084
        return

    # def _transform(self, request, record):
    #     payload = self._make_location_payload(record)
    #
    #     source_id = self.toST('location.properties.source_id', record)
    #     hvd = self.toST('location.properties.hydrovu_description', record)
    #
    #     payload['properties'] = {'agency': AGENCY,
    #                              'source_id': source_id,
    #                              'hydrovu.description': hvd}
    #     return payload


class PHVThings(HydroVuThings):
    _vocab_tag = 'phv'
    _tablename = 'pvacd_locations'
    _agency = AGENCY
    _where = "LOWER(name) like '%level%'"

    def _additional_properties(self, record):
        md = next((well for well in METADATA if well['name'] == record['name']), {})

        return {
            "nmbgmr_id": md.get('pointid'),
            "well_depth": {'value': md.get('welldepth'), 'unit': 'ft'},
            "hole_depth": {'value': md.get('holedepth'), 'unit': 'ft'}
        }


class PHVWaterLevelsDatastreams(HydroVuWaterLevelsDatastreams):
    _vocab_tag = 'phv'
    _agency = AGENCY
    _tablename = 'pvacd_locations'
    _where = "LOWER(name) like '%level%'"


class PHVObservations(HydroVuObservations):
    _vocab_tag = 'phv'
    _tablename = 'pvacd_readings'
    _agency = AGENCY


# class PHVThings(PHV_Site_STAO, ThingMixin):
#     _entity_tag = 'thing'
#
#     def _transform(self, request, record):
#         payload = self._make_thing_payload(record)
#         payload['properties'] = {'agency': AGENCY,
#                                  'source_id': self.toST('thing.properties.source_id', record)}
#
#         return payload
#
#
# class PHVWaterLevelsDatastreams(PHV_Site_STAO, DatastreamMixin):
#     _entity_tag = 'datastream'
#
#     def _transform(self, request, record):
#         payload = self._make_datastream_payload(record, 'gwl', AGENCY)
#         payload['properties'] = {}
#
#
# class PHVObservations(BQSTAO, ObservationMixin):
#     _tablename = 'pecos_readings'
#     _fields = ['value', 'unitId', 'timestamp',
#                'locationId', 'parameterId', 'customParameter', '_airbyte_extracted_at']
#     _limit = 500
#     _where = "parameterId=4"
#
#     _dataset = 'levels'
#     _entity_tag = 'observation'
#
#     _orderby = 'timestamp asc'
#     _location_field = 'locationId'
#     _cursor_id = '_airbyte_extracted_at'
#     _datastream_name = GWL_DS['name']
#     _thing_name = WATER_WELL['name']
#     _agency = AGENCY
#     _timestamp_field = 'timestamp'
#     _value_field = 'value'


if __name__ == '__main__':
    phv = PHVLocations()
    # phv = PHVThings()
    phv.render(None, dry=True)

# ============= EOF =============================================
