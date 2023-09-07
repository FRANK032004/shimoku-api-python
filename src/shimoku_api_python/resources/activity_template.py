from typing import Optional, Dict, TYPE_CHECKING

from ..base_resource import Resource

if TYPE_CHECKING:
    from .universe import Universe

import logging
from ..execution_logger import logging_before_and_after
logger = logging.getLogger(__name__)


class ActivityTemplate(Resource):

    resource_type = 'activityTemplate'
    alias_field = 'name'
    plural = 'activityTemplates'

    @logging_before_and_after(logger.debug)
    def __init__(self, parent: 'Universe', uuid: Optional[str] = None, alias: Optional[str] = None,
                 db_resource: Optional[Dict] = None):

        params = {
            'name': alias if alias else '',
            'description': '',
            'availableModeCost': {},
            'inputSettings': [],
            'minRunInterval': 0,
            'enabled': False,
            'provider': '',
        }

        super().__init__(
            parent=parent, uuid=uuid, db_resource=db_resource,
            params_to_serialize=['availableModeCost', 'inputSettings'], params=params)
