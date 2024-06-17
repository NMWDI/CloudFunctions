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
main.py.  This module holds all the cloud function entry points.
"""

try:
    from constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP, PRESSURE_SENSOR, ACOUSTIC_SENSOR, HYDROVU_SENSOR
    from constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP, PRESSURE_SENSOR, ACOUSTIC_SENSOR, \
        TOTALIZER_OBSERVED_PROPERTIES, TOTALIZER_SENSOR
    from stao import SimpleSTAO
except ImportError:
    from stao.constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP, PRESSURE_SENSOR, ACOUSTIC_SENSOR, HYDROVU_SENSOR
    from stao.constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP, PRESSURE_SENSOR, ACOUSTIC_SENSOR,\
        TOTALIZER_OBSERVED_PROPERTIES, TOTALIZER_SENSOR
    from stao.stao import SimpleSTAO

# ======================== bernco ===========================
def bernco_locations(request):
    from bernco.entities import BerncoLocations
    stao = BerncoLocations()
    return stao.render(request)

def bernco_things(request):
    from bernco.entities import BerncoThings
    stao = BerncoThings()
    return stao.render(request)

def bernco_waterlevel_datastreams(request):
    from bernco.entities import BernCoWaterLevelsDatastreams
    stao = BernCoWaterLevelsDatastreams()

    ss = SimpleSTAO()
    ss.render('sensor', HYDROVU_SENSOR)

    return stao.render(request)

# ======================== pvacd hydrovu ===========================
def pecos_hydrovu_locations(request):
    print('received request', request)
    from pecos_hydrovu.entities import PHVLocations
    stao = PHVLocations()
    resp =  stao.render(request)
    print('response', resp)
    return resp


def pecos_hydrovu_things(request):
    from pecos_hydrovu.entities import PHVThings
    stao = PHVThings()
    return stao.render(request)


def pecos_hydrovu_waterlevel_datastreams(request):
    from pecos_hydrovu.entities import PHVWaterLevelsDatastreams
    stao = PHVWaterLevelsDatastreams()

    ss = SimpleSTAO()
    ss.render('sensor', HYDROVU_SENSOR)

    return stao.render(request)


def pecos_hydrovu_water_levels(request):
    from pecos_hydrovu.entities import PHVObservations
    stao = PHVObservations()
    return stao.render(request)

# =================================================


# ======================== isc seven rivers ===========================
def isc_seven_rivers_locations(request):
    from isc_seven_rivers.entities import etl_locations
    return etl_locations(request)


def isc_seven_rivers_things(request):
    from isc_seven_rivers.entities import etl_things
    return etl_things(request)


def isc_seven_rivers_datastreams(request):
    from isc_seven_rivers.entities import ISCSevenRiversSensors, ISCSevenRiversObservedProperties, \
        ISCSevenRiversDatastreams
    ret = []
    for k in (ISCSevenRiversSensors, ISCSevenRiversObservedProperties, ISCSevenRiversDatastreams):
        stao = k()
        ret.append(stao.render(request))

    return ','.join(ret)


def isc_seven_rivers_water_levels(request):
    from isc_seven_rivers.entities import ISCSevenRiversWaterLevels
    stao = ISCSevenRiversWaterLevels()
    return stao.render(request)


# =============== NMBGMR =====================
def nmbgmr_locations(request):
    from nmbgmr.entities import NMBGMRLocations
    stao = NMBGMRLocations()
    return stao.render(request)


def nmbgmr_things(request):
    from nmbgmr.entities import NMBGMRThings
    stao = NMBGMRThings()
    return stao.render(request)


def nmbgmr_manual_waterlevel_datastreams(request):
    from nmbgmr.entities import NMBGMRManualWaterLevelsDatastreams
    stao = NMBGMRManualWaterLevelsDatastreams()

    ss = SimpleSTAO()
    ss.render('sensor', MANUAL_SENSOR)
    ss.render('observed_property', DTW_OBS_PROP)

    return stao.render(request)


def nmbgmr_pressure_waterlevel_datastreams(request):
    from nmbgmr.entities import NMBGMRPressureWaterLevelsDatastreams
    stao = NMBGMRPressureWaterLevelsDatastreams()

    ss = SimpleSTAO()
    ss.render('sensor', PRESSURE_SENSOR)
    ss.render('observed_property', DTW_OBS_PROP)

    return stao.render(request)


def nmbgmr_acoustic_waterlevel_datastreams(request):
    from nmbgmr.entities import NMBGMRAcousticWaterLevelsDatastreams
    stao = NMBGMRAcousticWaterLevelsDatastreams()

    ss = SimpleSTAO()
    ss.render('sensor', ACOUSTIC_SENSOR)
    ss.render('observed_property', DTW_OBS_PROP)

    return stao.render(request)


def nmbgmr_pressure_waterlevel_observations(request):
    from nmbgmr.entities import NMBGMRWaterLevelsObservations
    stao = NMBGMRWaterLevelsObservations('pressure_gwl')
    return stao.render(request)


def nmbgmr_acoustic_waterlevel_observations(request):
    from nmbgmr.entities import NMBGMRWaterLevelsObservations
    stao = NMBGMRWaterLevelsObservations('acoustic_gwl')
    return stao.render(request)


def nmbgmr_manual_waterlevel_observations(request):
    from nmbgmr.entities import NMBGMRWaterLevelsObservations
    stao = NMBGMRWaterLevelsObservations('nmbgmr_manual_gwl')
    return stao.render(request)
# =================================================


# =============== OSE RealTime =====================
def ose_realtime_locations(request):
    from ose_realtime.entities import OSERealtimeLocations
    stao = OSERealtimeLocations()
    return stao.render(request)


def ose_realtime_things(request):
    from ose_realtime.entities import OSERealtimeThings
    stao = OSERealtimeThings()
    return stao.render(request)


def ose_realtime_datastreams(request):
    from ose_realtime.entities import OSERealtimeSensors, OSERealtimeObservedProperties, OSERealtimeDatastreams

    ret = []
    for k in (OSERealtimeSensors, OSERealtimeObservedProperties, OSERealtimeDatastreams):
        stao = k()
        ret.append(stao.render(request))

    return ','.join(ret)
# =================================================


def isc_seven_rivers_totalizer_datastreams(request):
    from stao.isc_seven_rivers.entities import ISCSevenRiversTotalizerDatastreams

    ss = SimpleSTAO()
    ss.render('sensor', TOTALIZER_SENSOR)
    for obspropd in TOTALIZER_OBSERVED_PROPERTIES:
        payload = obspropd.fromkeys(('name', 'description', 'definition', 'properties'))
        ss.render('observed_property', payload)

    stao = ISCSevenRiversTotalizerDatastreams()
    return stao.render(request)


# =============== CABQ =====================
def cabq_waterelevations(request):
    from cabq.entities import CABQWaterElevations
    stao = CABQWaterElevations()
    ret = stao.render(request)
    return ret


def cabq_waterdepths(request):
    from cabq.entities import CABQWaterDepths
    stao = CABQWaterDepths()
    ret = stao.render(request)
    return ret

# ============== EBID ========================
def ebid_locations(request):
    from ebid.entities import EBIDLocations
    stao = EBIDLocations()
    return stao.render(request)


def ebid_waterlevels(request):
    from ebid.entities import EBIDGWLObservations
    stao = EBIDGWLObservations()
    return stao.render(request)
# ============= EOF =============================================
