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

try:
    from constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS, WELL_LOCATION_DESCRIPTION
except ImportError:
    from stao.constants import WATER_WELL, HYDROVU_SENSOR, DTW_OBS_PROP, GWL_DS, WELL_LOCATION_DESCRIPTION


def clean_name(name):
    """
    remove level/Level from the name
    :param name:
    :return:
    """
    return name.replace('level', '').replace('Level', '').replace(' -', '-')


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


class VocabMapper:
    def load(self, name):
        if name == 'phv':
            vb = PHV

        self._vocab = vb

    def toST(self, path, record=None, default=None):
        """
        location.latitude
        :param path:
        :return:
        """
        obj = self._vocab
        for p in path.split('.'):
            obj = obj[p]

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
