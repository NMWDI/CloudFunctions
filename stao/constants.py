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

NO_DESCRIPTION = 'No Description'
NO_DEFINITION = 'No Definition'
NO_METADATA = 'No Metadata'

DTW_OBS_PROP = {'name': 'Depth to Water Below Ground Surface',
                'description': 'depth to water below ground surface',
                'definition': NO_DEFINITION}
ELEV_OBS_PROP = {'name': 'Groundwater Elevation',
                 'description': 'Elevation of groundwater in feet above msl',
                 'definition': NO_DEFINITION
                 }

ENCODING_GEOJSON = "application/vnd.geo+json"
ENCODING_PDF = "application/pdf"

WATER_WELL = 'Water Well'
WATER_QUANTITY = 'Water Quantity'

MANUAL_SENSOR = {'name': 'Manual',
                 'description': NO_DESCRIPTION,
                 'encodingType': ENCODING_PDF,
                 'metadata': NO_METADATA
                 }

PRESSURE_SENSOR = {'name': 'Pressure',
                   'description': '''Continuous (periodic automated) measurement depth to water in Feet below ground 
                   surface (converted from pressure reading from depth below ground surface in feet). Not 
                   Provisional. Quality Controlled ''',
                   'encodingType': ENCODING_PDF,
                   'metadata': NO_METADATA
                   }

ACOUSTIC_SENSOR = {'name': 'Acoustic',
                   'description': '''Continuous (periodic automated) measurement depth to water in Feet below ground 
surface (converted from acoustic device). Not Provisional. Quality Controlled''',
                   'encodingType': ENCODING_PDF,
                   'metadata': NO_METADATA
                   }

GWL_DS = {'name': 'Groundwater Levels',
          'description': 'Measurement of groundwater depth in a water well, as measured below ground surface'}

GWE_DS = {'name': 'Groundwater Elevations',
          'description': 'Elevation of groundwater in feet above msl'}
# ============= EOF =============================================


