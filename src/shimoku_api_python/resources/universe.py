from typing import Optional, List

from ..base_resource import Resource
from .business import Business
from .activity_template import ActivityTemplate
from ..client import ApiClient

import logging
from ..execution_logger import logging_before_and_after, log_error
logger = logging.getLogger(__name__)


class Universe(Resource):
    resource_type = 'universe'

    @logging_before_and_after(logger.debug)
    def __init__(self, api_client: ApiClient, uuid: str):

        super().__init__(api_client=api_client, uuid=uuid, children=[Business, ActivityTemplate])

    @logging_before_and_after(logger.debug)
    async def create_universe_api_key(self, description: str):
        endpoint = self._base_resource.base_url + f'universe/{self._base_resource.id}/apiKey'

        params = dict(
            userType='ADMIN',
            enabled=True,
            description=description
        )

        return await self._base_resource.api_client.query_element(
            method='POST', endpoint=endpoint,
            **{'body_params': params}
        )

    @logging_before_and_after(logger.debug)
    async def get_universe_api_keys(self):
        endpoint = self._base_resource.base_url + f'universe/{self._base_resource.id}/apiKeys'

        return (await self._base_resource.api_client.query_element(
            method='GET', endpoint=endpoint,
        ))['items']

    # Business methods
    @logging_before_and_after(logger.debug)
    async def create_business(self, name: str, theme: Optional[dict] = None) -> Business:
        if self._base_resource.api_client.playground:
            log_error(logger, 'Cannot create business in local environment', RuntimeError)
        return await self._base_resource.create_child(Business, alias=name, theme=theme if theme else {})

    @logging_before_and_after(logger.debug)
    async def update_business(self, uuid: Optional[str] = None, name: Optional[str] = None, **params):
        if 'new_name' in params:
            params['new_alias'] = params.pop('new_name')
        if self._base_resource.api_client.playground:
            uuid, name = 'local', None
        return await self._base_resource.update_child(Business, uuid=uuid, alias=name, **params)

    @logging_before_and_after(logger.debug)
    async def get_business(self, uuid: Optional[str] = None, name: Optional[str] = None) -> Optional[Business]:
        if self._base_resource.api_client.playground:
            uuid, name = 'local', None
        return await self._base_resource.get_child(Business, uuid, name)

    @logging_before_and_after(logger.debug)
    async def get_businesses(self) -> List[Business]:
        return await self._base_resource.get_children(Business)

    @logging_before_and_after(logger.debug)
    async def delete_business(self, uuid: Optional[str] = None, name: Optional[str] = None):
        if self._base_resource.api_client.playground:
            log_error(logger, 'Cannot delete local business', RuntimeError)
        return await self._base_resource.delete_child(Business, uuid, name)

    # Activity template methods
    @logging_before_and_after(logger.debug)
    async def get_activity_template(
        self, uuid: Optional[str] = None, name: Optional[str] = None
    ) -> Optional[ActivityTemplate]:
        return await self._base_resource.get_child(ActivityTemplate, uuid, name)

    @logging_before_and_after(logger.debug)
    async def get_activity_templates(self) -> List[ActivityTemplate]:
        return await self._base_resource.get_children(ActivityTemplate)
