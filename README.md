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