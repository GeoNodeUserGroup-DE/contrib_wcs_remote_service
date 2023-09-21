#########################################################################
#
# Copyright (C) 2023 52°North Spatial Information Research GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################
from urllib.parse import (
    unquote,
    urlparse,
    urlencode,
    urlunparse,
    parse_qsl,
    ParseResult,
)
from urllib.request import urlopen
from xml.etree import ElementTree

from django.conf import settings
from owslib.wcs import WebCoverageService


def get_cleaned_url_params(url):
    # Unquoting URL first so we don't loose existing args
    url = unquote(url)
    # Extracting url info
    parsed_url = urlparse(url)
    # Extracting URL arguments from parsed URL
    get_args = parsed_url.query
    # Converting URL arguments to dict
    parsed_get_args = dict(parse_qsl(get_args))
    # Strip out redundant args
    _version = parsed_get_args.pop('version', '2.0.1') if 'version' in parsed_get_args else '2.0.1'
    _service = parsed_get_args.pop('service') if 'service' in parsed_get_args else None
    _request = parsed_get_args.pop('request') if 'request' in parsed_get_args else None
    # Converting URL argument to proper query string
    encoded_get_args = urlencode(parsed_get_args, doseq=True)
    # Creating new parsed result object based on provided with new
    # URL arguments. Same thing happens inside of urlparse.
    new_url = ParseResult(
        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
        parsed_url.params, encoded_get_args, parsed_url.fragment
    ).geturl()
    return new_url, _service, _version, _request


def get_wcs_service(url):
    cleaned_url, service, version, request = get_cleaned_url_params(url)
    ogc_server_settings = settings.OGC_SERVER['default']
    wcs = WebCoverageService(
        cleaned_url,
        version=version,
        timeout=ogc_server_settings.get('TIMEOUT', 60))
    return wcs


def get_wms_version(url):
    try:
        query_params = {
            'service': 'WMS',
            'request': 'GetCapabilities'
        }
        parsed = urlparse(url)
        parsed = parsed._replace(query=urlencode(query_params))
        url_wms = urlunparse(parsed)
        with urlopen(url_wms) as response:
            root = ElementTree.fromstring(response.read())
            return root.attrib['version']
    except Exception:
        return '1.3.0'