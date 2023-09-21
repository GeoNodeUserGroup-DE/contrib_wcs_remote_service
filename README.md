# Web Coverage Service (WCS) as a Remote Service in GeoNode

The WCS remote service app is a contrib module for GeoNode. The app is compatible with GeoNode v4.x.

It adds a service processor and harvester to allow importing layer metadata from remote WCS servers into GeoNode.
Only public WCS servers are supported.

## Installation and configuration

1) Install Python package

```shell
git clone https://github.com/GeoNodeUserGroup-DE/contrib_wcs_remote_service.git
cd contrib_wcs_remote_service
pip install --upgrade .
```

or directly

```shell
pip install git+https://github.com/GeoNodeUserGroup-DE/contrib_wcs_remote_service.git#egg=wcs_remote_service
```

2) Add WCS as remote service option in GeoNode

In settings.py add

```python
HARVESTER_CLASSES = ['wcs_remote_service.harvesters.wcs.WCSHarvester']
try:
    SERVICES_TYPE_MODULES = SERVICES_TYPE_MODULES + ['wcs_remote_service.serviceprocessors.wcs.WCSRemoteServiceRegistry']
except Exception:
    SERVICES_TYPE_MODULES = ['wcs_remote_service.serviceprocessors.wcs.WCSRemoteServiceRegistry']
```

References:
 * https://docs.geonode.org/en/master/intermediate/harvesting/index.html#creating-new-harvesting-workers
 * https://docs.geonode.org/en/master/basic/settings/index.html?highlight=SERVICES_TYPE_MODULES#services-type-modules

## Credits

This contrib app is based on the WMS remote service implementation of GeoNode.

## Funding

|                                                                      Project/Logo                                                                      | Description                                                                                                                                              |
|:------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| [<img alt="JKI" align="middle" width="267" height="50" src="https://www.julius-kuehn.de/assets/img/logo+lettering.svg"/>](https://www.julius-kuehn.de/) | This contrib app is funded by the Julius Kühn-Institut (JKI)                                                                                             |