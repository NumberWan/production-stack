# Copyright 2024-2025 The vLLM Production Stack Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import time
from typing import List, Tuple, Dict, Any, Optional
import aiohttp
import json

from vllm_router.log import init_logger

logger = init_logger(__name__)


class LMCacheHTTPClient:
    """
    HTTP client for connecting to LMCache Controller via HTTP API.
    This replaces the ZMQ-based LMCacheControllerManager to avoid port conflicts.
    """
    
    def __init__(self, controller_url: str):
        """
        Initialize the HTTP client.
        
        Args:
            controller_url: URL of the LMCache Controller (e.g., "http://localhost:9000")
        """
        self.controller_url = controller_url.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        logger.info(f"Initializing LMCacheHTTPClient with URL: {self.controller_url}")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def full_lookup(self, tokens: List[int]) -> Dict[str, Any]:
        """
        Perform a full lookup for the given tokens.
        
        Args:
            tokens: List of token IDs to lookup
            
        Returns:
            Dictionary containing matched_info and chunk_size
        """
        try:
            session = await self._get_session()
            url = f"{self.controller_url}/full_lookup"
            data = {"tokens": tokens}
            
            logger.debug(f"Sending full_lookup request to {url} with {len(tokens)} tokens")
            
            async with session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.debug(f"Full lookup response: {result}")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"Full lookup failed with status {response.status}: {error_text}")
                    return {"matched_info": [], "chunk_size": 256}
        
        except Exception as e:
            logger.error(f"Full lookup error: {e}")
            return {"matched_info": [], "chunk_size": 256}
    
    async def lookup(self, tokens: List[int]) -> Dict[str, Any]:
        """
        Perform a lookup for the given tokens.
        
        Args:
            tokens: List of token IDs to lookup
            
        Returns:
            Dictionary containing layout_info
        """
        try:
            session = await self._get_session()
            url = f"{self.controller_url}/lookup"
            data = {"tokens": tokens}
            
            logger.debug(f"Sending lookup request to {url} with {len(tokens)} tokens")
            
            async with session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.debug(f"Lookup response: {result}")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"Lookup failed with status {response.status}: {error_text}")
                    return {"event_id": "", "layout_info": {}}
        
        except Exception as e:
            logger.error(f"Lookup error: {e}")
            return {"event_id": "", "layout_info": {}}
    
    async def health_check(self, instance_id: str) -> Dict[str, Any]:
        """
        Check the health of a specific instance.
        
        Args:
            instance_id: ID of the instance to check
            
        Returns:
            Dictionary containing error_codes
        """
        try:
            session = await self._get_session()
            url = f"{self.controller_url}/health"
            data = {"instance_id": instance_id}
            
            logger.debug(f"Sending health check request to {url} for instance {instance_id}")
            
            async with session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.debug(f"Health check response: {result}")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"Health check failed with status {response.status}: {error_text}")
                    return {"event_id": "", "error_codes": {}}
        
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return {"event_id": "", "error_codes": {}}
    
    def __del__(self):
        """Cleanup on deletion."""
        if self.session and not self.session.closed:
            # Note: This is not ideal but necessary for cleanup
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close())
            except RuntimeError:
                pass
