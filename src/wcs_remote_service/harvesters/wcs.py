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
import logging
import typing
import uuid
from datetime import datetime

from django.conf import settings
from django.contrib.gis import geos
from django.template.defaultfilters import slugify
from geonode.base.models import ResourceBase, TopicCategory
from geonode.harvesting import models
from geonode.harvesting import resourcedescriptor
from geonode.harvesting.harvesters import base
from geonode.layers.enumerations import GXP_PTYPES
from geonode.layers.models import Dataset
# from geonode.thumbs.thumbnails import create_thumbnail

# from ..utils import get_wcs_service, get_wms_version
from ..utils import get_wcs_service

logger = logging.getLogger(__name__)


class WCSHarvester(base.BaseHarvesterWorker):
    """Harvester for resources coming from OGC WCS web services"""

    dataset_title_filter: typing.Optional[str]

    def __init__(
            self,
            *args,
            dataset_title_filter: typing.Optional[str] = None,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dataset_title_filter = dataset_title_filter

    @property
    def allows_copying_resources(self) -> bool:
        return False

    @classmethod
    def from_django_record(cls, record: models.Harvester):
        return cls(
            record.remote_url,
            record.id,
            dataset_title_filter=record.harvester_type_specific_configuration.get(
                'dataset_title_filter')
        )

    @classmethod
    def get_extra_config_schema(cls) -> typing.Optional[typing.Dict]:
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://geonode.org/harvesting/wcs-harvester.schema.json",
            "title": "OGC WCS harvester config",
            "description": (
                "A jsonschema for validating configuration option for GeoNode's "
                "remote OGC WCS harvester"
            ),
            "type": "object",
            "properties": {
                "dataset_title_filter": {
                    "type": "string",
                }
            },
            "additionalProperties": False,
        }

    def get_num_available_resources(self) -> int:
        return len(self._get_wcs().contents)

    def list_resources(
            self,
            offset: typing.Optional[int] = 0
    ) -> typing.List[base.BriefRemoteResource]:

        # look at `tasks.update_harvestable_resources()` in order to understand the purpose of the
        # `offset` parameter. Briefly, we try to retrieve resources in batches and we use `offset` to
        # control the pagination of the remote service. Unfortunately WMS does not really have the
        # concept of pagination and dumps all available coverages in a single `GetCapabilities` response.
        # With this in mind, we only handle the case where `offset == 0`, which returns all available resources
        # and simply return an empty list when `offset != 0`
        if offset != 0:
            return []

        resources = []
        for coverage_name, coverage in self._get_wcs().contents.items():

            if coverage.title is None:
                title = coverage_name
            else:
                title = coverage.title

            if coverage.abstract is None:
                abstract = 'Not provided'
            else:
                abstract = coverage.abstract

            resources.append(
                base.BriefRemoteResource(
                    unique_identifier=coverage_name,
                    title=title,
                    abstract=abstract,
                    resource_type='layers',
                )
            )
        return resources

    def check_availability(self, timeout_seconds: typing.Optional[int] = 5) -> bool:
        try:
            return True if len(self._get_wcs().contents) > 0 else False
        except Exception:
            return False

    def get_geonode_resource_type(self, remote_resource_type: str) -> ResourceBase:
        """Return resource type class from resource type string."""
        return Dataset

    def get_geonode_resource_defaults(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: models.HarvestableResource,  # noqa
    ) -> typing.Dict:
        defaults = super().get_geonode_resource_defaults(harvested_info, harvestable_resource)
        defaults['name'] = harvested_info.resource_descriptor.identification.name
        if harvested_info.resource_descriptor.identification.temporal_extent:
            defaults['temporal_extent_start'] = harvested_info.resource_descriptor.identification.temporal_extent[0]
            defaults['temporal_extent_end'] = harvested_info.resource_descriptor.identification.temporal_extent[1]
        defaults.update(harvested_info.resource_descriptor.additional_parameters)
        return defaults

    def get_resource(
            self,
            harvestable_resource: models.HarvestableResource,
    ) -> typing.Optional[base.HarvestedResourceInfo]:
        resource_unique_identifier = harvestable_resource.unique_identifier
        result = None
        try:
            wcs = self._get_wcs()
            contact_info = self._get_contact(wcs.provider.contact)
            relevant_coverage = [coverage_id for coverage_id in wcs.contents if coverage_id == resource_unique_identifier][0]
            coverage_metadata = self._get_metadata(relevant_coverage)
        except IndexError:
            logger.exception(f"Could not find resource {resource_unique_identifier!r}")
        else:
            # WCS does not provide uuid, so needs to generated on the first time
            # for update, use uuid from geonode resource
            resource_uuid = uuid.uuid4()
            if harvestable_resource.geonode_resource:
                resource_uuid = uuid.UUID(harvestable_resource.geonode_resource.uuid)
            time = datetime.now()
            service_name = slugify(self.remote_url)[:255]
            contact = resourcedescriptor.RecordDescriptionContact(contact_info)
            result = base.HarvestedResourceInfo(
                resource_descriptor=resourcedescriptor.RecordDescription(
                    uuid=resource_uuid,
                    point_of_contact=contact,
                    author=contact,
                    date_stamp=time,
                    identification=resourcedescriptor.RecordIdentification(
                        name=coverage_metadata['name'],
                        title=coverage_metadata['title'],
                        date=time,
                        date_type='',
                        originator=contact,
                        place_keywords=[],
                        other_keywords=coverage_metadata['keywords'],
                        topic_category=coverage_metadata['category'],
                        license=[],
                        abstract=coverage_metadata['abstract'],
                        spatial_extent=coverage_metadata['spatial_extent'],
                        temporal_extent=coverage_metadata['temporal_extent']
                    ),
                    distribution=resourcedescriptor.RecordDistribution(
                        wcs_url=coverage_metadata['wcs_url'],
                    ),
                    reference_systems=[coverage_metadata['crs']],
                    additional_parameters={
                        'alternate': coverage_metadata['name'],
                        'store': service_name,
                        'workspace': 'remoteWorkspace',
                        'ows_url': coverage_metadata['wcs_url'],
                        'ptype': GXP_PTYPES['WCS']
                    }
                ),
                additional_information=None
            )
        return result

    def finalize_resource_update(
            self,
            geonode_resource: ResourceBase,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: models.HarvestableResource
    ) -> ResourceBase:
        return geonode_resource
        # Create a thumbnail with a WMS request
        # if not geonode_resource.srid:
        #     target_crs = settings.DEFAULT_MAP_CRS
        # elif 'EPSG:' in str(geonode_resource.srid).upper() or 'CRS:' in str(geonode_resource.srid).upper():
        #     target_crs = geonode_resource.srid
        # else:
        #     target_crs = f'EPSG:{geonode_resource.srid}'
        # # wms_version = harvested_info.resource_descriptor
        # wms_version = get_wms_version(self.remote_url)
        # create_thumbnail(
        #     instance=geonode_resource,
        #     wms_version=wms_version,
        #     bbox=geonode_resource.bbox,
        #     forced_crs=target_crs,
        #     overwrite=True,
        # )

    def _get_wcs(self):
        return get_wcs_service(self.remote_url)

    def _get_wcs_content_metadata(self, coverage_id):
        """
        :param coverage_id:
        :return: dict
        """
        # owslib.coverage.wcsxxx.ContentMetadata
        try:
            wcs = self._get_wcs()
            wcs_content = wcs.contents[coverage_id]
        except Exception:
            msg = "No coverage with id {}".format(coverage_id)
            logger.error(msg)
            raise Exception(msg)

        name = getattr(wcs_content, 'id', None)
        if name is None:
            msg = "Coverage has no id"
            logger.error(msg)
            raise Exception(msg)

        bbox = self._get_bbox(wcs_content)
        spatial_extent = bbox['spatial_extent']

        try:
            temporal_extent = bbox['temporal_extent']
        except Exception as e:
            temporal_extent = None
            logger.debug(e)

        wcs_content_metadata = {
            'name': name,
            'title': getattr(wcs_content, 'title', None),
            'keywords': getattr(wcs_content, 'keywords', []),
            'abstract': getattr(wcs_content, 'abstract', None),
            'wcs_url': wcs.url,
            'spatial_extent': spatial_extent,
            'crs': bbox['crs'],
            'temporal_extent': (temporal_extent[0], temporal_extent[1]) if temporal_extent else None,
        }
        return wcs_content_metadata


    def _getOtherBoundingBoxes(self, coverage_id):
        """
        Adapted method '_getOtherBoundingBoxes' (originally from https://github.com/geopython/OWSLib/blob/master/owslib/coverage/wcs201.py)
        in order to parse time correctly
        :return:
        """

        bboxes = []

        try:
            describe_coverage = self._get_wcs().getDescribeCoverage(coverage_id)
        except Exception:
            return bboxes

        x_labels = ['longitude', 'lon', 'long', 'e', 'w', 'x']
        y_labels = ['latitude', 'lat', 'n', 's', 'y']
        # time_labels = ['ansi']

        for envelope in describe_coverage.findall(
                '{http://www.opengis.net/wcs/2.0}CoverageDescription/' +
                '{http://www.opengis.net/gml/3.2}boundedBy/' +
                '{http://www.opengis.net/gml/3.2}Envelope'
        ):
            bbox = {}
            bbox['nativeSrs'] = envelope.attrib['srsName']
            dims = int(envelope.attrib['srsDimension'])
            axis_labels = envelope.attrib['axisLabels'].lower().split()
            lc = envelope.find('{http://www.opengis.net/gml/3.2}lowerCorner')
            lc = lc.text.split()
            uc = envelope.find('{http://www.opengis.net/gml/3.2}upperCorner')
            uc = uc.text.split()
            if dims == 2:
                if axis_labels[0] in y_labels and axis_labels[1] in x_labels:
                    bbox['bbox'] = (float(lc[1]), float(lc[0]), float(uc[1]), float(uc[0]))
                else:
                    bbox['bbox'] = (float(lc[0]), float(lc[1]), float(uc[0]), float(uc[1]))
            elif dims == 3:
                # assumed time is always in first position
                if axis_labels[1] in y_labels and axis_labels[2] in x_labels:
                    bbox['bbox'] = (float(lc[2]), float(lc[1]), float(uc[2]), float(uc[1]))
                else:
                    bbox['bbox'] = (float(lc[1]), float(lc[2]), float(uc[1]), float(uc[2]))
                bbox['temporal_extent'] = (lc[0].replace('"', ''), uc[0].replace('"', ''))
            else:
                bbox['bbox'] = (float(lc[0]), float(lc[1]), float(uc[0]), float(uc[1]))
            bboxes.append(bbox)

        return bboxes

    def _get_metadata(self, coverage_id):

        metadata = self._get_wcs_content_metadata(coverage_id)

        if metadata['title'] is None:
            metadata['title'] = metadata['name']

        if metadata['abstract'] is None:
            metadata['abstract'] = 'Not provided'

        metadata['category'] = None
        for keyword in metadata['keywords']:
            category = self._get_category(keyword)
            if category:
                metadata['category'] = category
                break

        return metadata

    def _get_bbox(self, wcs_content):
        """
        Get spatial and temporal bounding box
        :param wcs_content: owslib.coverage.wcsxxx.ContentMetadata
        """

        bbox = {}

        # Check if fields defined by a method (using property()) are available
        try:
            if len(wcs_content.boundingboxes) > 0:
                has_boundingboxes = True
            else:
                has_boundingboxes = False
        except Exception:
            has_boundingboxes = False

        try:
            other_boundingboxes = self._getOtherBoundingBoxes(wcs_content.id)
            if len(other_boundingboxes) == 0:
                other_boundingboxes = False
        except Exception:
            other_boundingboxes = False

        # ToDo: implement parsing grid
        # try:
        #     if wcs_content.grid is not None:
        #         has_grid = True
        #     else:
        #         has_grid = False
        # except Exception:
        #     has_grid = False

        if wcs_content.boundingBox:
            # wcs_content.boundingBox is always None (at least until owslib version 0.27.2)
            left_x = wcs_content.boundingBox[0]
            lower_y = wcs_content.boundingBox[1]
            right_x = wcs_content.boundingBox[2]
            upper_y = wcs_content.boundingBox[3]
            # bbox['crs'] = 'EPSG:?'
            bbox['temporal_extent'] = None
        elif wcs_content.boundingBoxWGS84:
            left_x = wcs_content.boundingBoxWGS84[0]
            lower_y = wcs_content.boundingBoxWGS84[1]
            right_x = wcs_content.boundingBoxWGS84[2]
            upper_y = wcs_content.boundingBoxWGS84[3]
            bbox['crs'] = 'EPSG:4326'
            bbox['temporal_extent'] = None
        elif has_boundingboxes:
            # What if there is more than one bounding box?
            left_x = wcs_content.boundingboxes[0]['bbox'][0]
            lower_y = wcs_content.boundingboxes[0]['bbox'][1]
            right_x = wcs_content.boundingboxes[0]['bbox'][2]
            upper_y = wcs_content.boundingboxes[0]['bbox'][3]
            srid_url = wcs_content.boundingboxes[0]['nativeSrs']
            bbox['crs'] = '{}:{}'.format(srid_url.split('/')[-3], srid_url.split('/')[-1])
            bbox['temporal_extent'] = None
        elif other_boundingboxes:
            # What if there is more than one bounding box?
            left_x = other_boundingboxes[0]['bbox'][0]
            lower_y = other_boundingboxes[0]['bbox'][1]
            right_x = other_boundingboxes[0]['bbox'][2]
            upper_y = other_boundingboxes[0]['bbox'][3]
            srid_url = other_boundingboxes[0]['nativeSrs']
            bbox['crs'] = '{}:{}'.format(srid_url.split('/')[-3], srid_url.split('/')[-1])
            bbox['temporal_extent'] = other_boundingboxes[0]['temporal_extent'] \
                if other_boundingboxes[0]['temporal_extent'] else None
        # elif has_grid:
        #     bbox['spatial_extent'] = wcs_content.grid
        #     bbox['crs'] = 'EPSG:?'
        #     bbox['temporal_extent'] = None
        else:
            left_x = -180.0
            lower_y = -90.0
            right_x = 180.0
            upper_y = 90.0
            bbox['crs'] = 'EPSG:4326'
            bbox['temporal_extent'] = None

        bbox['spatial_extent'] = geos.Polygon.from_bbox((left_x, lower_y, right_x, upper_y, ))

        return bbox

    def _get_category(self, category_string):
        # we don't implement semantic matching of categories; identifier must exactly match (case-sensitive!), otherwise no category is set
        try:
            category_qs =  TopicCategory.objects.filter(identifier=category_string)
            if category_qs:
                return category_qs[0]
            else:
                return None
        except Exception:
            return None

    def _get_contact(self, contact) -> dict:
        """Return contact from owslib.ows.ServiceContact object"""
        return {
            "role": "",
            "name": contact.name,
            "organization": contact.organization,
            "position": contact.position,
            "phone_voice": contact.phone,
            "address_delivery_point": "",
            "address_city": contact.city,
            "address_administrative_area": contact.region,
            "address_postal_code": contact.postcode,
            "address_country": contact.country,
            "address_email":contact.email,
        }
