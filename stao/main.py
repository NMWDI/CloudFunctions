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
import functions_framework

try:
    from constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP
    from stao import SimpleSTAO
except ImportError:
    from stao.constants import NO_DESCRIPTION, MANUAL_SENSOR, DTW_OBS_PROP
    from stao.stao import SimpleSTAO


def isc_seven_rivers_locations(request):
    from isc_seven_rivers.entities import etl_locations
    return etl_locations(request)


def isc_seven_rivers_things(request):
    from isc_seven_rivers.entities import etl_things
    return etl_things(request)


@functions_framework.http
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


def isc_seven_rivers_datastreams(request):
    from isc_seven_rivers.entities import ISCSevenRiversSensors, ISCSevenRiversObservedProperties, ISCSevenRiversDatastreams
    ret = []
    for k in (ISCSevenRiversSensors, ISCSevenRiversObservedProperties, ISCSevenRiversDatastreams):
        stao = k()
        ret.append(stao.render(request))

    return ','.join(ret)


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
# ============= EOF =============================================
