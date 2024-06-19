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
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
        LOCATION_DESCRIPTION, asiotid, make_statime
    from hydrovu import HydroVuLocations, HydroVuThings, HydroVuWaterLevelsDatastreams, HydroVuObservations
except ImportError:
    from stao.constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO, ObservationMixin, LocationMixin, ThingMixin, \
        DatastreamMixin
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid, make_statime
    from stao.hydrovu import HydroVuLocations, HydroVuThings, HydroVuWaterLevelsDatastreams, HydroVuObservations

AGENCY = 'BernCo'


# class BernCo_Site_STAO(BQSTAO):
#     _vocab_tag = 'BernCo'
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


class BernCoLocations(HydroVuLocations):
    _agency = AGENCY
    _vocab_tag = 'phv'
    _tablename = 'bernco_locations'


class BernCoThings(HydroVuThings):
    _vocab_tag = 'phv'
    _agency = AGENCY
    _tablename = 'bernco_locations'


class BernCoWaterLevelsDatastreams(HydroVuWaterLevelsDatastreams):
    _vocab_tag = 'phv'
    _agency = AGENCY
    _tablename = 'bernco_locations'


class BernCoObservations(HydroVuObservations):
    _vocab_tag = 'phv'
    _tablename = 'bernco_readings'
    _agency = AGENCY


if __name__ == '__main__':
    BernCo = BernCoLocations()
    # BernCo = BernCoThings()
    BernCo.render(None, dry=True)
# ============= EOF =============================================