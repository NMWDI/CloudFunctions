# CloudFunctions

# Workflow
1. create a new package ```foo``` within ```stao```
2. create a new module ```entities.py``` within ```stao/foo```
3. create new STAO classes for each ST entity
4. create a new function in ```main.py``` that imports, instantiates and ```renders``` the necessary STAOs


# Example
```python
def nmbgmr_locations(request):
    from nmbgmr.entities import NMBGMRLocations
    stao = NMBGMRLocations()
    return stao.render(request)
```



# Uploading Observations
1. Create an `ObservationsSTAO`.  Must be a `STAO` subclass and also inherit `ObservationMixin`
```python
class ISCSevenRiversWaterLevels(BQSTAO, ObservationMixin):
```
2. Class must define the following attributes
   1. _tablename --- `'isc_water_levels'`
   2. _fields --- ```['dry', 'invalid', 'comments',
               'monitoring_point_id', 'dateTime', 'depthToWaterFeet',
               '_airbyte_ab_id'] ```   
   3. _dataset --- `'levels'`
   5. _timestamp_field --- `'dateTime'`
   6. _value_field --- `'depthToWaterFeet'`
   7. _cursor_id --- `'_airbyte_ab_id'`
   8. _location_field --- `'monitoring_point_id'` 
   9. _agency --- `'ISC_SEVEN_RIVERS'`
   10. _thing_name --- `'Water Well'`
   11. _datastream_name --- `'Groundwater Levels'`
3. The following attributes are optionals
   1. _limit --- `500`
   2. _orderby --- `'_airbyte_ab_id asc'`
   3. _where  --- `'parameterId=4'`
4. Create a new function in ```main.py``` and render the STAO normally.
   ```python
   def isc_seven_rivers_water_levels(request):
       from isc_seven_rivers.entities import ISCSevenRiversWaterLevels
       stao = ISCSevenRiversWaterLevels()
       return stao.render(request)
```