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
from uuid import uuid4

from django.db import transaction
from django.template.defaultfilters import slugify
from django.utils.translation import ugettext as _
from geonode.services import models
from geonode.services.enumerations import INDEXED
from geonode.services.serviceprocessors import base
from geonode.harvesting.models import Harvester

from ..utils import get_wcs_service

logger = logging.getLogger(__name__)


class WCSServiceHandler(base.ServiceHandlerBase):
    """Remote service handler for WCS services"""

    service_type = 'WCS'
    harvester_type = 'wcs_remote_service.harvesters.wcs.WCSHarvester'

    def __init__(self, url, geonode_service_id=None):
        base.ServiceHandlerBase.__init__(self, url, geonode_service_id)
        self.indexing_method = INDEXED
        self.name = slugify(self.url)[:255]

    @property
    def wcs(self):
        return get_wcs_service(self.url)

    def probe(self):
        try:
            return True if len(self.wcs.contents) > 0 else False
        except Exception:
            return False

    def create_geonode_service(self, owner, parent=None):
        """Create a new geonode.service.models.Service instance
        :arg owner: The user who will own the service instance
        :type owner: geonode.people.models.Profile
        """
        with transaction.atomic():
            instance = models.Service.objects.create(
                uuid=str(uuid4()),
                base_url=self.url,
                type=self.service_type,
                method=self.indexing_method,
                owner=owner,
                metadata_only=True,
                version=str(self.wcs.identification.version).encode("utf-8", "ignore").decode('utf-8'),
                name=self.name,
                title=str(self.wcs.identification.title).encode("utf-8", "ignore").decode('utf-8') or self.name,
                abstract=str(self.wcs.identification.abstract).encode("utf-8", "ignore").decode('utf-8') or _(
                    "Not provided"),
            )
            service_harvester = Harvester.objects.create(
                name=self.name,
                default_owner=owner,
                scheduling_enabled=False,
                remote_url=instance.service_url,
                delete_orphan_resources_automatically=True,
                harvester_type=self.harvester_type,
                harvester_type_specific_configuration=self.get_harvester_configuration_options()
            )
            if service_harvester.update_availability():
                service_harvester.initiate_update_harvestable_resources()
            else:
                logger.exception(GeoNodeException("Could not reach remote endpoint."))
            instance.harvester = service_harvester

        self.geonode_service_id = instance.id
        return instance

    def get_keywords(self):
        return self.wcs.identification.keywords


class WCSRemoteServiceRegistry:
    """Helper class. Only used to import new service types."""

    services_type = {
        'WCS': {
            'OWS': False,
            'handler': WCSServiceHandler,
            'label': 'Web Coverage Service',
            # 'management_view': ''
        }
    }
