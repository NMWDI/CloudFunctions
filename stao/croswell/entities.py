# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import datetime
import io
from itertools import groupby

from pandas import read_csv

from stao.base_stao import BaseSTAO, LocationGeoconnexMixin, LocationMixin, ThingMixin, DatastreamMixin, \
    ObservationMixin, BucketSTAO, MultifileBucketSTAO
from stao.constants import WATER_QUANTITY, WATER_WELL, GWL_DS


class LocalSTAO(BaseSTAO):

    def _extract(self, request):
        df = read_csv(self._path, delimiter=',', header=0)
        for i, row in df.iterrows():
            record = row.to_dict()
            record = self._extract_hook(record)
            yield record

    def _extract_hook(self, record):
        return record


class BubblerSTAO(LocalSTAO):
    pass


class CityRoswellSTAO(BubblerSTAO):
    _agency = 'CityOfRoswell'
    _vocab_tag = 'city_of_roswell'
    _path = 'data/Roswell_locations.csv'


class CityRoswellBucketSTAO(MultifileBucketSTAO):
    _bucket_name = 'roswellbubbler'
    _agency = 'CityOfRoswell'
    _vocab_tag = 'city_of_roswell'



class CityRoswellLocationSTAO(LocationGeoconnexMixin, LocationMixin, CityRoswellSTAO):
    def _make_location_properties(self, record):
        return {
            # 'name': f'Site-{record["site_id"]}',
            # 'description': '',
            'geometry': {
                'type': 'Point',
                'coordinates': [record['x_coord'], record['y_coord']]
            }
        }

class CityRoswellThingSTAO(ThingMixin, CityRoswellSTAO):

    def _get_location(self, record):
        name = f'Site-{record["site_id"]}'
        location = self._client.get_location(name=name)
        return location

    def _make_thing_properties(self, record):
        return {
            'well_depth': {
                'value': record['well_depth'],
                'unit': record['well_depth_unit']
            },
            'casing_diameter': {
                'value': record['casing_diameter'],
                'unit': record['casing_diameter_unit']
            }
        }


class CityRoswellDatastreamSTAO(DatastreamMixin, CityRoswellSTAO):
    def _transform(self, request, record):
        payload = self._make_datastream_payload(record, 'gwl', self._agency)
        payload['properties'] = {'topic': WATER_QUANTITY,
                                 'agency': self._agency,
                                 'is_continuous': True,
                                 'is_provisional': True}
        return payload

    # def _get_thing(self, record, agency):
    #     name = record['name']
    #     return self._client.get_thing(name=f'Groundwater Level Monitoring Point - {name}')

class CityRoswellObservationSTAO(ObservationMixin, CityRoswellBucketSTAO):
    # _path = 'data/SMW18_measurements_fixed.csv'
    # _location_field = 'site_id'
    _thing_name = WATER_WELL['name']
    _timestamp_field = 'Unnamed: 3'
    _value_field = 'depth_to_water'
    _datastream_name = GWL_DS['name']

        # def key(r):
        #     print(r)
        #     return r['site_id']

        # ds = list(self._get_dict_iter())
        # for site_id, gs in groupby(sorted(ds, key=key), key=key):
        #     yield {'site_id': site_id, 'observations': list(gs)}
    # def _extract(self, request):
    #     def key(r):
    #         return r['site_id']
    #
    #     df = read_csv(self._path, delimiter=',', header=0)
    #     ds = [row.to_dict() for i, row in df.iterrows()]
    #
    #     for site_id, gs in groupby(sorted(ds, key=key), key=key):
    #         yield {'site_id': site_id, 'observations': list(gs)}

    def _handle_extract(self, blobcontent):
        df = read_csv(io.StringIO(blobcontent.decode('utf-8')), delimiter=',', header=0)
        # for i, row in df.iterrows():
        #     record = row.to_dict()
        #     yield record
        return {'observations': df.to_dict(orient='records'), 'site_id': 18}

    def _extract_timestamp(self, dt):
        return dt

    def _transform_timestamp(self, dt):
        return datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')

    def _get_location(self, record, location_id=None):
        name = f'Site-{record["site_id"]}'
        location = self._client.get_location(name=name)
        return location, name


if __name__ == '__main__':
    # c = CityRoswellLocationSTAO()
    # c = CityRoswellThingSTAO()
    # c = CityRoswellDatastreamSTAO()
    c = CityRoswellObservationSTAO()

    c.render(None, dry=False)
    # c.render(None, dry=True)
# ============= EOF =============================================
