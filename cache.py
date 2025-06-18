import requests

import asyncio

from dataclasses import dataclass

from igvf_async_client import AsyncIgvfApi

from typing import List, Dict, Any, Tuple

import logging

logger = logging.getLogger(__name__)


async def get_by_id(api: AsyncIgvfApi, at_id: str) -> Tuple[str, Dict[str, Any]]:
    r = await api.get_by_id(at_id)
    return (at_id, r.actual_instance.to_dict())

@dataclass
class PortalCacheProps:
    base_url: str
    async_portal_api: AsyncIgvfApi


class PortalCache:

    def __init__(self, props: PortalCacheProps):
        self.props = props
        self.local = {}

    def preload(self, searches: List[str]):
        for search in searches:
            results = requests.get(
                self.props.base_url + search
            ).json()['@graph']
            print(f'Got {len(results)} results from {search}')
            for result in results:
                self.local[result['@id']] = result

    async def async_batch_get(self, at_ids: List[str], load_cache: bool = True) -> Dict[str, Any]:
        cached_ids = [
            at_id
            for at_id in at_ids
            if at_id in self.local
        ]
        logger.info(f'Found {len(cached_ids)} out of {len(at_ids)} at_ids cached')
        results =  dict(
            await asyncio.gather(
                *(
                    get_by_id(
                        self.props.async_portal_api,
                        at_id
                    ) for at_id in at_ids
                    if at_id not in cached_ids
                )
            )
        )
        logger.info(f'Got {len(results)} remote results')
        if results and load_cache:
            logger.info('Updating cache')
            self.local.update(results)
        if cached_ids:
            results.update(
                {
                    at_id: self.local[at_id]
                    for at_id in cached_ids
                }
            )
        if len(set(at_ids)) != len(results):
            raise ValueError(
                f'Mismatch in at_ids and results length: {len(set(at_ids))} at_ids != {len(results)} results'
            )
        return results


'''
PRELOAD_SEARCHES = [
    '/search/?type=RodentDonor&limit=all&frame=object',
]
import logging
logging.basicConfig(level=logging.INFO)
from igvf_async_client import AsyncIgvfApi
api = AsyncIgvfApi()
from cache import PortalCacheProps
from cache import PortalCache
p = PortalCacheProps(base_url='https://api.data.igvf.org', async_portal_api=api)
pc = PortalCache(props=p)
pc.preload(PRELOAD_SEARCHES)
ids = ['/rodent-donors/IGVFDO0813DZTW/', '/rodent-donors/IGVFDO4725SNCJ/', '/rodent-donors/IGVFDO3534MDFF/', '/rodent-donors/IGVFDO2319OUEC/']
await pc.async_batch_get(ids)
'''
