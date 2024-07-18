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
from sta.definitions import FOOT, OM_Measurement
from stao.constants import (WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP,
                            GWL_DS, WELL_LOCATION_DESCRIPTION, MANUAL_GWL_DS)


def tofloat(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return v


def parse_screen_interval(v):
    try:
        l, h = v.split('-')
        return {'bottom': float(h.strip()), 'top': float(l.strip())}
    except (ValueError, AttributeError):
        pass


def clean_name(name):
    """
    remove level/Level from the name
    :param name:
    :return:
    """
    return name.replace('level', '').replace('Level', '').replace(' -', '-')


MANUAL = {'sensor': {'name': {'text': 'Manual'}},
          'observed_property': {'name': {'text': DTW_OBS_PROP['name']}},
          'unitOfMeasurement': {'text': FOOT},
          'observationType': {'text': OM_Measurement},
          'datastream': {'name': {'text': MANUAL_GWL_DS['name']},
                         'description': {'text': MANUAL_GWL_DS['description']}}
          }

BERNCO = {'location': {'latitude': 'latitude',
                       'longitude': 'longitude',
                       'description': {'text': WELL_LOCATION_DESCRIPTION},
                       'name': {'column': 'name',
                                # 'postprocess': clean_name
                                },
                       'properties': {'source_id': 'well_uuid',
                                      'ose_permit': 'ose_permit',
                                      'nmbgmr_id': 'point_id'}},

          'thing': {'name': {'text': WATER_WELL['name']},
                    'description': {'text': WATER_WELL['description']},
                    'properties': {'source_id': 'well_uuid',
                                   'well_uuid': 'well_uuid',
                                   'ose_permit': 'ose_permit',
                                   'nmbgmr_id': 'point_id',
                                   'aquifer_code': 'aquifer_code',
                                   'well_depth': {'column': 'well_depth', 'postprocess': tofloat},
                                   'casing_stickup': {'column': 'casing_stickup', 'postprocess': tofloat},
                                   'screen_interval': {'column': 'screen_interval',
                                                       'postprocess': parse_screen_interval}}},
          'manual': MANUAL
          }

PHV = {'location': {'latitude': 'latitude',
                    'longitude': 'longitude',
                    'description': {'text': WELL_LOCATION_DESCRIPTION},
                    'name': {'column': 'name',
                             # 'postprocess': clean_name
                             },
                    'properties': {'source_id': 'id',
                                   'hydrovu_description': 'description'}},

       'thing': {'name': {'text': WATER_WELL['name']},
                 'description': {'text': WATER_WELL['description']},
                 'properties': {'source_id': 'id'}},
       'gwl': {'sensor': {'name': {'text': HYDROVU_SENSOR['name']}},
               'observed_property': {'name': {'text': DTW_OBS_PROP['name']}},
               'unitOfMeasurement': {'text': FOOT},
               'observationType': {'text': OM_Measurement},
               'datastream': {'name': {'text': GWL_DS['name']},
                              'description': {'text': GWL_DS['description']}}}
       }

PECOS_MANUAL = {
    'location': {'latitude': 'latitude',
                 'longitude': 'longitude',
                 'description': {'text': WELL_LOCATION_DESCRIPTION},
                 'name': {'column': 'name',
                          # 'postprocess': clean_name
                          },
                 'properties': {'source_id': 'id',
                                'hydrovu_description': 'description'}},

    'thing': {'name': {'text': WATER_WELL['name']},
              'description': {'text': WATER_WELL['description']},
              'properties': {'source_id': 'id'}},
    'manual': MANUAL,
}


class VocabMapper:
    def load(self, name):
        vb = {}
        if name == 'phv':
            vb = PHV
        elif name == 'pecos_manual':
            vb = PECOS_MANUAL
        elif name == 'bernco':
            vb = BERNCO

        self._vocab = vb

    def toST(self, path, record=None, default=None):
        """
        location.latitude
        :param path:
        :return:
        """
        obj = self._vocab

        for p in path.split('.'):
            try:
                obj = obj[p]
            except KeyError as e:
                print('failed to find path', path, p, obj)
                raise e
        out = None
        postprocess = None
        if isinstance(obj, dict):
            key = obj.get('column')
            if not key:
                out = obj.get('text')
            else:
                postprocess = obj.get('postprocess')

        else:
            key = obj

        if key:
            out = record.get(key, default)

        if postprocess:
            out = postprocess(out)
        return out


def vocab_factory(name):
    v = VocabMapper()
    v.load(name)
    return v
# ============= EOF =============================================
