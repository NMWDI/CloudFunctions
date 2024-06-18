# ===============================================================================
# Copyright 2024 Jake Ross
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

try:
    from constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
        DatastreamMixin
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
        asiotid, make_statime
except ImportError:
    print('hudyasd, import error', e)
    from stao.constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
        DatastreamMixin
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid, make_statime



class HydroVu_Site_STAO(BQSTAO):
    # _vocab_tag = 'HydroVu'
    # _tablename = 'pecos_locations'

    _fields = ['id', 'name', 'latitude', 'longitude', 'description']

    _dataset = 'nmwdi'
    _limit = 100
    _orderby = 'id asc'
    _where = "LOWER(name) like '%level%'"

    def _transform_message(self, record):
        return f"id={record['id']}, name={record['name']}"


class HydroVuLocations(LocationGeoconnexMixin, HydroVu_Site_STAO, LocationMixin):
    _entity_tag = 'location'

    def _transform(self, request, record):
        payload = self._make_location_payload(record)

        source_id = self.toST('location.properties.source_id', record)
        hvd = self.toST('location.properties.hydrovu_description', record)

        payload['properties'] = {'agency': self._agency,
                                 'source_id': source_id,
                                 'hydrovu.description': hvd}
        return payload


class HydroVuThings(HydroVu_Site_STAO, ThingMixin):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        payload = self._make_thing_payload(record)
        payload['properties'] = {'agency': self._agency,
                                 'source_id': self.toST('thing.properties.source_id', record)}

        return payload


class HydroVuWaterLevelsDatastreams(HydroVu_Site_STAO, DatastreamMixin):
    _entity_tag = 'datastream'

    def _transform(self, request, record):
        payload = self._make_datastream_payload(record, 'gwl', self._agency)
        payload['properties'] = {}
        return payload


class HydroVuObservations(ObservationMixin, BQSTAO):
    # _tablename = 'bernco_readings'
    _fields = ['value', 'unitId', 'timestamp',
               'locationId', 'parameterId', 'customParameter', '_airbyte_extracted_at']
    _limit = 500
    _where = "parameterId=4"

    _dataset = 'nmwdi'
    _entity_tag = 'observation'

    _orderby = 'timestamp asc'
    _location_field = 'locationId'
    _cursor_id = 'timestamp'
    _datastream_name = GWL_DS['name']
    _thing_name = WATER_WELL['name']

    _timestamp_field = 'timestamp'
    _value_field = 'value'

    def _extract_timestamp(self, dt):
        return dt
# ============= EOF =============================================