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
# try:
from stao.base_stao import BQSTAO, DatastreamMixin, ObservationMixin
from stao.constants import MANUAL_GWL_DS, WATER_WELL

# from stao.base_stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
#     DatastreamMixin
# from ..util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
#     asiotid, make_statime
# except ImportError as e:
#     print('import error', e)
#     from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
#         DatastreamMixin
#     from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
#         make_fuzzy_geometry_from_latlon, asiotid, make_statime

AGENCY = 'PVACD'
LOCATION_IDS = {
    1524: 'Artesia A Level',
    1515: 'Poe Corn Level',
    1516: 'Transwestern Level' ,
    1517: 'Berrendo-Smith level',
    1518: 'LFD Level',
    1519: 'Orchard Park Level',
    1520: 'Greenfield level',
    1521: 'Bartlett level',
    1522: 'Cottonwood level',
    1523: 'Zumwalt level',
}


class PecosManualWaterLevelsObservations(ObservationMixin, BQSTAO):
    _agency = AGENCY
    _vocab_tag = 'pecos_manual'
    _dataset = 'nmwdi'
    _tablename = 'pvacdmetermanager_WellMeasurements'
    _fields = ['_airbyte_extracted_at', 'id', 'value', 'unit_id', 'well_id', 'timestamp']
    _limit = 500
    _entity_tag = 'observation'

    _orderby = '_airbyte_extracted_at asc'

    _location_field = 'well_id'
    _cursor_id = '_airbyte_extracted_at'

    _datastream_name = MANUAL_GWL_DS['name']
    _thing_name = WATER_WELL['name']

    _timestamp_field = 'timestamp'
    _value_field = 'value'

    def _get_location(self, record, **kw):
        lid = LOCATION_IDS.get(record['locationId'])

        q = f"name eq '{lid}' and properties/agency eq '{self._agency}'"
        return self._client.get_location(query=q), lid

    def _transform_value(self, v, record):
        if record['unit_id'] == 7:
            # convert meters to feet
            return v * 3.28084

        return v

    def _transform_timestamp(self, dt):
        return dt

    def _extract_timestamp(self, dt):
        return dt

    def _get_cursor(self, record):
        return record.get(self._cursor_id).isoformat()


class PecosManualWaterlevelsDatastreams(BQSTAO, DatastreamMixin):
    _vocab_tag = 'pecos_manual'
    _dataset = 'nmwdi'
    _tablename = 'pvacdmetermanager_Wells'
    _fields = ['id', 'name', 'osetag']
    _agency = AGENCY
    _where = 'id in (1524, 1521, 1517, 1522, 1520, 1518, 1519, 1515, 1516, 1523)'
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        record = dict(record)
        if record['name'] == 'Artesia':
            record['name'] = 'Artesia A Level'
        elif record['name'] == 'LFD':
            record['name'] = 'LFD Level '
        elif record['name'] == 'OrchardPark':
            record['name'] = 'Orchard Park Level'
        elif record['name'] == 'PoeCorn':
            record['name'] = 'Poe Corn Level'
        elif record['name'] == 'TransWestern':
            record['name'] = 'Transwestern Level'
        else:
            record['name'] = f'{record["name"]} level'

        payload = self._make_datastream_payload(record, 'manual', self._agency)
        payload['properties'] = {}
        return payload

# ============= EOF =============================================
