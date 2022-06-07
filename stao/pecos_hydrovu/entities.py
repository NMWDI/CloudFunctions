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

try:
    from constants import WATER_WELL
    from stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO
    from util import make_geometry_point_from_utm, make_geometry_point_from_latlon, make_fuzzy_geometry_from_latlon, \
        LOCATION_DESCRIPTION, asiotid
except ImportError:
    from stao.constants import WATER_WELL
    from stao.stao import LocationGeoconnexMixin, BQSTAO, BaseSTAO
    from stao.util import make_geometry_point_from_utm, make_geometry_point_from_latlon, \
        make_fuzzy_geometry_from_latlon, asiotid


def clean_name(name):
    """
    remove level/Level from the name
    :param name:
    :return:
    """
    return name.replace('level','').replace('Level', '')


class PHV_Site_STAO(BQSTAO):
    _fields = ['id', 'name', 'latitude', 'longitude', 'description']

    _dataset = 'locations'
    _tablename = 'pecos_locations'

    _limit = 100
    _orderby = 'id asc'
    _where = "LOWER(name) like '%level%'"

    def _transform_message(self, record):
        return f"id={record['id']}, name={record['name']}"


class PHVLocations(LocationGeoconnexMixin, PHV_Site_STAO):
    _entity_tag = 'location'

    def _transform(self, request, record):
        properties = {}
        properties['agency'] = 'PVACD'
        properties['source_id'] = record['id']
        properties['hydrovu.description'] = record['description']
        lat = record['latitude']
        lon = record['longitude']

        payload = {'name': clean_name(record['name']),
                   'description': 'Location of well where measurements are made',
                   'properties': properties,
                   'location': make_geometry_point_from_latlon(lat, lon),
                   "encodingType": "application/vnd.geo+json",
                   }

        return payload


class PHVThings(PHV_Site_STAO):
    _entity_tag = 'thing'

    def _transform(self, request, record):
        name = clean_name(record['name'])
        location = self._client.get_location(f"name eq '{name}'")

        payload = {'name': WATER_WELL['name'],
                   'Locations': [{'@iot.id': location['@iot.id']}],
                   'description': WATER_WELL['description'],
                   'properties': {'agency': 'PVACD',
                                  'source_id': record['id']}
                   }

        return payload


if __name__ == '__main__':
    phv = PHVLocations()
    phv.render(None, dry=True)

# ============= EOF =============================================
