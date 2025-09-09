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

import abc
import asyncio
import enum
import math
import random
import threading
import traceback
import time
import csv
import os
from typing import Dict, List, Optional

from fastapi import Request

try:
    from transformers import AutoTokenizer
except ImportError:
    pass

try:
    from lmcache.v1.cache_controller import controller_manager
    from lmcache.v1.cache_controller.message import (
        LookupMsg,
        FullLookupMsg,
        QueryInstMsg,
    )
except ImportError:
    pass
from uhashring import HashRing

from vllm_router.log import init_logger
from vllm_router.monitoring.request_timing import get_request_timing_monitor
from vllm_router.service_discovery import EndpointInfo
from vllm_router.stats.engine_stats import EngineStats
from vllm_router.stats.request_stats import RequestStats
from vllm_router.utils import SingletonABCMeta

logger = init_logger(__name__)


def extract_prompt(request_json: Dict):
    """Extract prompt message from the request json object."""
    if "messages" in request_json:
        # Get the last message from the messages array
        messages = request_json["messages"]
        if messages:
            # Concatenate all message content
            prompt_parts = []
            for message in messages:
                content = message.get("content", "")
                if isinstance(content, list):
                    # Handle multimodal messages
                    text_content = " ".join(
                        part.get("text", "")
                        for part in content
                        if part.get("type") == "text"
                    )
                    prompt_parts.append(text_content)
                elif content is not None:
                    prompt_parts.append(content)
            return "\n".join(prompt_parts)
        return ""
    # Handle regular completions
    return request_json["prompt"]


class RoutingLogic(str, enum.Enum):
    ROUND_ROBIN = "roundrobin"
    SESSION_BASED = "session"
    KVAWARE = "kvaware"
    PREFIXAWARE = "prefixaware"
    DISAGGREGATED_PREFILL = "disaggregated_prefill"
    TTFT = "ttft"


class RoutingInterface(metaclass=SingletonABCMeta):

    def _qps_routing(
        self, endpoints: List[EndpointInfo], request_stats: Dict[str, RequestStats]
    ) -> str:
        """
        Route the request to the appropriate engine URL based on the QPS of
        each engine

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            request_stats (Dict[str, RequestStats]): The request stats
                indicating the request-level performance of each engine
        """
        lowest_qps = float("inf")
        ret = None
        for info in endpoints:
            url = info.url
            if url not in request_stats:
                return url  # This engine does not have any requests
            request_stat = request_stats[url]
            if request_stat.qps < lowest_qps:
                lowest_qps = request_stat.qps
                ret = url
        return ret

    def _update_hash_ring(self, endpoints: List["EndpointInfo"]):
        """
        Update the hash ring with the current list of endpoints.
        """
        # Extract endpoint URLs
        endpoint_urls = [endpoint.url for endpoint in endpoints]

        # Get the current nodes in the hash ring
        current_nodes = set(self.hash_ring.get_nodes())

        # Convert the new endpoint URLs to a set for easy comparison
        new_nodes = set(endpoint_urls)

        # Remove nodes that are no longer in the list
        for node in current_nodes - new_nodes:
            self.hash_ring.remove_node(node)

        # Add new nodes that are not already in the hash ring
        for node in new_nodes - current_nodes:
            self.hash_ring.add_node(node)

    @abc.abstractmethod
    def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
    ) -> str:
        """
        Route the request to the appropriate engine URL

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
                the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
                indicating the request-level performance of each engine
            request (Request): The incoming request
        """
        raise NotImplementedError


class RoundRobinRouter(RoutingInterface):
    # TODO (ApostaC): when available engines in the endpoints changes, the
    # algorithm may not be "perfectly" round-robin.
    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self.req_id = 0
        self.sorted_endpoints = []
        self.last_endpoints_id = None
        self.last_endpoints_hash = None
        self._initialized = True

    async def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
        request_json: Dict = None,
    ) -> str:
        """
        Route the request to the appropriate engine URL using a simple
        round-robin algorithm with optional LMCache lookup for monitoring

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
                the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
                indicating the request-level performance of each engine
            request (Request): The incoming request
            request_json (Dict, optional): The request body for LMCache lookup
        """
        endpoints_id = id(endpoints)
        if endpoints_id != self.last_endpoints_id:
            current_hash = hash(tuple(e.url for e in endpoints))
            if current_hash != self.last_endpoints_hash:
                self.sorted_endpoints = sorted(endpoints, key=lambda e: e.url)
                self.last_endpoints_hash = current_hash
            self.last_endpoints_id = endpoints_id
        chosen = self.sorted_endpoints[self.req_id % len(self.sorted_endpoints)]
        self.req_id += 1

        # 執行非侵入式 LMCache lookup 用於監控（不影響路由決策）
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"RR route_request called with request_json: {request_json is not None}")
        if request_json is not None:
            await self._perform_lmcache_lookup(request, request_json, endpoints)

        return chosen.url

    async def _perform_lmcache_lookup(self, request: Request, request_json: Dict, endpoints: List[EndpointInfo]):
        """執行 LMCache lookup 用於監控，重用 KvawareRouter 的邏輯"""
        import logging
        logger = logging.getLogger(__name__)
        logger.info("RR _perform_lmcache_lookup called")
        
        try:
            # 檢查是否有 LMCache controller port
            lmcache_port = getattr(request.app.state, 'lmcache_controller_port', None)
            logger.info(f"RR lmcache_port: {lmcache_port}")
            if lmcache_port is None:
                logger.info("RR lmcache_port is None, skipping lookup")
                return

            # 獲取 timing_data
            timing_data = getattr(request.state, 'timing_data', None)
            logger.info(f"RR timing_data: {timing_data is not None}")
            if timing_data is None:
                logger.info("RR timing_data is None, skipping lookup")
                return

            # 重用 KvawareRouter 的 lookup 邏輯
            from lmcache.v1.cache_controller import controller_manager  # type: ignore
            from lmcache.v1.cache_controller.message import LookupMsg  # type: ignore
            from transformers import AutoTokenizer  # type: ignore
            from vllm_router.monitoring.request_timing import get_request_timing_monitor
            import math
            import time

            # 步驟1: Tokenize（重用 KvawareRouter 邏輯）
            tokenize_start = time.time()
            if not hasattr(request.app.state, '_rr_tokenizer'):
                request.app.state._rr_tokenizer = AutoTokenizer.from_pretrained(endpoints[0].model_names[0])
            tokenizer = request.app.state._rr_tokenizer
            token_ids = tokenizer.encode(extract_prompt(request_json))
            tokenize_time = time.time() - tokenize_start

            # 步驟2: Lookup（重用 KvawareRouter 邏輯）
            lookup_start = time.time()
            if not hasattr(request.app.state, '_lmcache_manager'):
                request.app.state._lmcache_manager = controller_manager.LMCacheControllerManager(f"0.0.0.0:{lmcache_port}")
            
            kv_mgr = request.app.state._lmcache_manager
            msg = LookupMsg(event_id="", tokens=token_ids)
            # 現在是異步環境，可以直接 await
            instance_id = await kv_mgr.handle_orchestration_message(msg)
            
            matched_tokens = math.inf
            if instance_id and len(list(instance_id.layout_info.keys())) > 0:
                matched_instance_id = list(instance_id.layout_info.keys())[0]
                matched_tokens = instance_id.layout_info[matched_instance_id][1]
            
            lookup_time = time.time() - lookup_start

            # 寫入到 timing_data（重用 KvawareRouter 邏輯）
            timing_data.lookup_time = lookup_time
            timing_data.matched_kvcache_tokens = int(matched_tokens if matched_tokens != math.inf else 0)
            timing_data.request_tokens = int(len(token_ids))
            
            # 估算：未命中部分可能需傳輸的 token 數（監控用，不影響路由）
            uncached_tokens = max(0, int(len(token_ids)) - int(matched_tokens if matched_tokens != math.inf else 0))
            if uncached_tokens > 0:
                timing_data.kv_cache_transfer_count += 1
                timing_data.kv_cache_transfer_tokens += int(uncached_tokens)
            
            # 記錄 lookup 結果
            timing_monitor = get_request_timing_monitor()
            timing_monitor.record_kv_cache_lookup(
                timing_data,
                lookup_time=lookup_time,
                hit=bool(matched_tokens != math.inf and matched_tokens > 0),
            )
            
            logger.info(f"RR lookup completed: tokens={len(token_ids)}, matched={matched_tokens if matched_tokens != math.inf else 0}, hit={bool(matched_tokens != math.inf and matched_tokens > 0)}")
            
        except Exception as e:
            # 記錄錯誤以便診斷
            logger.info(f"RR LMCache lookup failed: {e}")
            pass


class SessionRouter(RoutingInterface):
    """
    Route the request to the appropriate engine URL based on the session key
    in the request headers
    """

    def __init__(self, session_key: str = None):
        if hasattr(self, "_initialized"):
            return
        if session_key is None:
            raise ValueError("SessionRouter must be initialized with a session_key")
        self.session_key = session_key
        self.hash_ring = HashRing()
        self._initialized = True

    def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
    ) -> str:
        """
        Route the request to the appropriate engine URL by the 'session id' in
        the request headers.
        If there is no session id in the request header, it will pick a server
        with lowest qps

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
                the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
                indicating the request-level performance of each engine
            request (Request): The incoming request
        """
        session_id = request.headers.get(self.session_key, None)
        logger.debug(f"Got session id: {session_id}")

        # Update the hash ring with the current list of endpoints
        self._update_hash_ring(endpoints)

        if session_id is None:
            # Route based on QPS if no session ID is present
            url = self._qps_routing(endpoints, request_stats)
        else:
            # Use the hash ring to get the endpoint for the session ID
            url = self.hash_ring.get_node(session_id)

        return url


class KvawareRouter(RoutingInterface):
    """
    Route the request to the appropriate engine URL by where the KV cache
    of the longest prefix match is found.
    """

    def __init__(
        self,
        lmcache_controller_port: int,
        session_key: str,
        kv_aware_threshold: int = 2000,
        tokenizer_name: Optional[str] = None,
        instance_id_to_url: Optional[Dict[str, str]] = None,
    ):
        self.lmcache_controller_port = lmcache_controller_port
        logger.info(
            f"Initializing KvawareRouter with port: {self.lmcache_controller_port}"
        )
        self.kv_manager = controller_manager.LMCacheControllerManager(
            f"0.0.0.0:{self.lmcache_controller_port}"
        )
        self.req_id = 0
        if instance_id_to_url is None:
            self.instance_id_to_url = {}
        else:
            self.instance_id_to_url = instance_id_to_url
        self.session_key = session_key
        self.hash_ring = HashRing()
        self.tokenizer_name = tokenizer_name
        self.tokenizer = None
        self.threshold = kv_aware_threshold
        # 移除舊的性能監控變量，現在使用統一的 RequestTimingMonitor

    def start_kv_manager(self):
        """
        Start the kv manager
        """
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.kv_manager.start_all(), self.loop)
        if self.tokenizer_name is not None:
            self.tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_name)

    # 移除舊的 CSV 寫入方法，現在使用統一的 RequestTimingMonitor

    def query_manager(self, msg) -> str:
        """
        Get the instance id for the given message
        """
        instance_id = self.kv_manager.handle_orchestration_message(msg)
        return instance_id

    async def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
        request_json: Dict,
    ) -> str:
        """
        Route the request to the appropriate engine URL by where the KV cache
        of the longest prefix match is found.
        If there is no session id in the request header, it will pick a server
        with round robin.

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
               the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
               indicating the request-level performance of each engine
            request (Request): The incoming request
            request_json (Dict): The request body (needed for finding the
            longest prefix match)
        """
        # 開始性能監控
        start_time = time.time()
        
        # 獲取全局時間追蹤監控器
        timing_monitor = get_request_timing_monitor()
        timing_data = {
            'total_time': 0,
            'tokenize_time': 0,
            'lookup_time': 0,
            'hash_routing_time': 0,
            'instance_mapping_time': 0,
            'find_best_matched_time': 0,
            'find_best_ttft_time': 0,
            'fallback_time': 0
        }
        
        # 步驟1: Tokenize !!!!!!!!!!!!!!!
        tokenize_start = time.time()
        if self.tokenizer is None:
            self.tokenizer = AutoTokenizer.from_pretrained(endpoints[0].model_names[0])
        url = endpoints[0].url + "/tokenize"
        # TODO (Yuhan): Handle chat completions
        token_ids = self.tokenizer.encode(extract_prompt(request_json))
        timing_data['tokenize_time'] = time.time() - tokenize_start
        
        # 步驟2: Lookup（僅計 LookupMsg 送出到返回的用時） !!!!!!!!!!!!!!!
        lookup_start = time.time()
        msg = LookupMsg(event_id="", tokens=token_ids)
        instance_id = await self.query_manager(msg)
        matched_tokens = math.inf
        if len(list(instance_id.layout_info.keys())) > 0:
            matched_instance_id = list(instance_id.layout_info.keys())[
                0
            ]  # Get the first key
            matched_tokens = instance_id.layout_info[matched_instance_id][1]
        timing_data['lookup_time'] = time.time() - lookup_start
        # 寫入到全局簡化輸出（如有 timing_data）
        try:
            request_timing_data = getattr(request.state, 'timing_data', None)
            if request_timing_data is not None:
                request_timing_data.lookup_time = timing_data['lookup_time']
                request_timing_data.matched_kvcache_tokens = int(matched_tokens if matched_tokens != math.inf else 0)
                request_timing_data.request_tokens = int(len(token_ids))
        except Exception:
            pass

        if (
            instance_id is None
            or len(instance_id.layout_info) == 0
            or matched_tokens < max(len(token_ids) - self.threshold, 0)
        ):
            # 步驟3: Hash routing !!!!!!!!!!!!!!!
            hash_routing_start = time.time()
            session_id = request.headers.get(self.session_key, None)
            logger.debug(f"Got session id: {session_id}")

            # Update the hash ring with the current list of endpoints
            self._update_hash_ring(endpoints)

            if session_id is None:
                # Route based on QPS if no session ID is present
                url = self._qps_routing(endpoints, request_stats)
            else:
                # Use the hash ring to get the endpoint for the session ID
                url = self.hash_ring.get_node(session_id)
            timing_data['hash_routing_time'] = time.time() - hash_routing_start
            
            # 計算總時間
            timing_data['total_time'] = time.time() - start_time
            
            # 更新全局時間追蹤
            request_timing_data = getattr(request.state, 'timing_data', None)
            if request_timing_data:
                timing_monitor.update_router_timing(request_timing_data, timing_data)
            
            return url
        else:
            # 步驟4: Instance mapping
            instance_mapping_start = time.time()
            queried_instance_ids = [info for info in instance_id.layout_info]
            if queried_instance_ids[0] not in self.instance_id_to_url:
                for endpoint in endpoints:
                    query_message = QueryInstMsg(
                        event_id="",
                        ip=endpoint.url.split(f":{endpoint.url.split(':')[-1]}")[
                            0
                        ].split("//")[1]
                    )
                    endpoint_instance_id = await self.query_manager(query_message)

                    self.instance_id_to_url[endpoint_instance_id.instance_id] = (
                        endpoint.url
                    )
                logger.info(f"Instance id to ip: {self.instance_id_to_url}")
            logger.info(
                f"Routing request to {queried_instance_ids[0]} found by kvaware router"
            )
            timing_data['instance_mapping_time'] = time.time() - instance_mapping_start
            
            # 計算總時間
            timing_data['total_time'] = time.time() - start_time
            
            # 更新全局時間追蹤
            request_timing_data = getattr(request.state, 'timing_data', None)
            if request_timing_data:
                timing_monitor.update_router_timing(request_timing_data, timing_data)
            
            return self.instance_id_to_url[queried_instance_ids[0]]


class PrefixAwareRouter(RoutingInterface):
    """
    Route the request to the appropriate engine URL by where the longest
    prefix match is found.

    In this class, we assume that there is no eviction of prefix cache.
    """

    def __init__(self: int):
        if hasattr(self, "_initialized"):
            return
        from vllm_router.prefix.hashtrie import HashTrie

        self.hashtrie = HashTrie()
        self._initialized = True
        # 移除舊的性能監控變量，現在使用統一的 RequestTimingMonitor

    async def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
        request_json: Dict,
    ) -> str:
        """
        Route the request to the appropriate engine URL by where the longest
        prefix match is found.

        In this routing logic, we do not consider the eviction of prefix cache.

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
               the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
               indicating the request-level performance of each engine
            request (Request): The incoming request
            request_json (Dict): The request body (needed for finding the
            longest prefix match)
        """
        # 開始性能監控
        start_time = time.time()
        timing_data = {
            'total_time': 0,
            'tokenize_time': 0,
            'lookup_time': 0,
            'hash_routing_time': 0,
            'instance_mapping_time': 0,
            'find_best_matched_time': 0,
            'find_best_ttft_time': 0,
            'fallback_time': 0
        }
        
        # 步驟1: Extract prompt !!!!!!!!!!!!!!!
        extract_start = time.time()
        prompt = extract_prompt(request_json)
        timing_data['tokenize_time'] = time.time() - extract_start
        
        # 步驟2: Longest prefix match（視作 Prefix 的 lookup） !!!!!!!!!!!!!!!
        match_start = time.time()
        available_endpoints = set(endpoint.url for endpoint in endpoints)
        _, matched_endpoint = await self.hashtrie.longest_prefix_match(
            prompt, available_endpoints
        )
        timing_data['lookup_time'] = time.time() - match_start
        # 寫入到全局簡化輸出（如有 timing_data）
        try:
            request_timing_data = getattr(request.state, 'timing_data', None)
            if request_timing_data is not None:
                request_timing_data.lookup_time = timing_data['lookup_time']
        except Exception:
            pass
        
        # 步驟3: Select endpoint !!!!!!!!!!!!!!!
        select_start = time.time()
        selected_endpoint = random.choice(list(matched_endpoint))
        timing_data['hash_routing_time'] = time.time() - select_start
        
        # 步驟4: Insert to hashtrie !!!!!!!!!!!!!!!
        insert_start = time.time()
        await self.hashtrie.insert(prompt, selected_endpoint)
        timing_data['instance_mapping_time'] = time.time() - insert_start
        
        # 計算總時間
        timing_data['total_time'] = time.time() - start_time
        
        # 移除舊的 CSV 寫入邏輯，現在使用統一的 RequestTimingMonitor

        return selected_endpoint


class DisaggregatedPrefillRouter(RoutingInterface):
    """
    Route the request to the appropriate engine URL by handling prefill and decode operations sequentially.
    First request goes to prefill endpoint, then second request goes to decode endpoint.
    """

    def __init__(self, prefill_model_labels: List[str], decode_model_labels: List[str]):
        self.prefill_model_labels = prefill_model_labels
        self.decode_model_labels = decode_model_labels
        self.request_cache = {}  # Cache to store prefill results

    def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
        request_json: Dict,
    ) -> str:
        """
        Route the request to appropriate endpoints for prefill and decode operations.
        First request goes to prefill endpoint, then second request goes to decode endpoint.
        """
        # Find prefill and decode endpoints
        is_prefill = request_json.get("max_tokens", 0) == 1
        if is_prefill:
            logger.info("Prefill request")
        else:
            logger.info("Decode request")

        # Find endpoints with matching model labels
        prefiller_endpoints = [
            e for e in endpoints if e.model_label in self.prefill_model_labels
        ]
        decoder_endpoints = [
            e for e in endpoints if e.model_label in self.decode_model_labels
        ]
        if is_prefill:
            return prefiller_endpoints[0].url
        else:
            return decoder_endpoints[0].url


class TtftRouter(RoutingInterface):
    """
    Route the request to the qppropriate engine URL by the least estimated TTFT.
    """

    def __init__(
        self,
        lmcache_controller_port: int,
        session_key: str,
        tokenizer_name: Optional[str] = None,
        instance_id_to_url: Optional[Dict[str, str]] = None,
    ):
        logger.info(
            f"Initializing TtftRouter with lmcache addr: 0.0.0.0:{lmcache_controller_port}"
        )
        self.kv_manager = controller_manager.LMCacheControllerManager(
            f"0.0.0.0:{lmcache_controller_port}"
        )
        if instance_id_to_url is None:
            self.instance_id_to_url = {}
        else:
            self.instance_id_to_url = instance_id_to_url
        self.session_key = session_key
        self.hash_ring = HashRing()
        self.tokenizer_name = tokenizer_name
        self.tokenizer = None
        self.uncached_prefix_tokens = None
        # 移除舊的性能監控變量，現在使用統一的 RequestTimingMonitor

    def start_kv_manager(self):
        """
        Start the kv manager
        """
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.kv_manager.start_all(), self.loop)
        if self.tokenizer_name is not None:
            self.tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_name)

    # 移除舊的 CSV 寫入方法，現在使用統一的 RequestTimingMonitor

    async def route_request(
        self,
        endpoints: List[EndpointInfo],
        engine_stats: Dict[str, EngineStats],
        request_stats: Dict[str, RequestStats],
        request: Request,
        request_json: Dict,
    ) -> str:
        """
        Route the request to the appropriate engine URL by where the KV cache
        of the longest prefix match is found.
        If there is no session id in the reqest header, it will pick a server
        with round robin.

        Args:
            endpoints (List[EndpointInfo]): The list of engine URLs
            engine_stats (Dict[str, EngineStats]): The engine stats indicating
                the 'physical' load of each engine
            request_stats (Dict[str, RequestStats]): The request stats
                indicating the request-level performance of each engine
            request (Request): The incoming request
            request_json (Dist): The request body (needed for finding the
            longest prefix match)
        """
        # 開始性能監控
        start_time = time.time()
        
        # 獲取全局時間追蹤監控器
        timing_monitor = get_request_timing_monitor()
        timing_data = {
            'total_time': 0,
            'tokenize_time': 0,
            'lookup_time': 0,
            'find_best_matched_time': 0,
            'find_best_ttft_time': 0,
            'fallback_time': 0
        }
        
        try:
            # 步驟1: Tokenize !!!!!!!!!!!!!!!
            tokenize_start = time.time()
            if self.tokenizer is None:
                # fallback to use the model of the first endpoint as tokenizer
                self.tokenizer = AutoTokenizer.from_pretrained(endpoints[0].model_names[0])

            token_ids = self.tokenizer.encode(extract_prompt(request_json))
            timing_data['tokenize_time'] = time.time() - tokenize_start
            
            # 步驟2: Lookup !!!!!!!!!!!!!!!
            lookup_start = time.time()
            if request_stats is None:
                raise ValueError("no request stats was provided")
            # 僅計 FullLookupMsg 的用時
            full_lookup_start = time.time()
            msg = FullLookupMsg(event_id="", tokens=token_ids)
            ret_msg = await self.kv_manager.handle_orchestration_message(msg)
            matched_infos = ret_msg.matched_info
            timing_data['lookup_time'] = time.time() - full_lookup_start
            # 寫入到全局簡化輸出（如有 timing_data）
            try:
                request_timing_data = getattr(request.state, 'timing_data', None)
                if request_timing_data is not None:
                    request_timing_data.lookup_time = timing_data['lookup_time']
                    # 從 matched_infos 取最大匹配 token 數
                    try:
                        best = None
                        for instance_info in matched_infos:
                            score = instance_info[1][-1][1]
                            if best is None or score > best:
                                best = score
                        matched_tokens = int(best or 0)
                    except Exception:
                        matched_tokens = 0
                    request_timing_data.matched_kvcache_tokens = matched_tokens
                    # TTFT 路由同樣使用 token_ids 長度
                    request_timing_data.request_tokens = int(len(token_ids))
            except Exception:
                pass
            
            if matched_infos:
                # 步驟3: Find best matched !!!!!!!!!!!!!!!
                find_best_start = time.time()
                best_matched_info = self._find_best_matched(matched_infos)
                self.uncached_prefix_tokens = len(token_ids) - best_matched_info[1][-1][1]
                timing_data['find_best_matched_time'] = time.time() - find_best_start
                
                # 步驟4: Find best TTFT !!!!!!!!!!!!!!!
                find_ttft_start = time.time()
                best_ttft_url = await self._find_best_ttft(endpoints, matched_infos,
                                                           best_matched_info, request_stats)
                timing_data['find_best_ttft_time'] = time.time() - find_ttft_start
                
                # 計算總時間
                timing_data['total_time'] = time.time() - start_time
                
                # 更新全局時間追蹤
                request_timing_data = getattr(request.state, 'timing_data', None)
                if request_timing_data:
                    timing_monitor.update_router_timing(request_timing_data, timing_data)
                
                # 移除舊的 CSV 寫入邏輯，現在使用統一的 RequestTimingMonitor
                
                return best_ttft_url
        except ValueError:
            logger.info("Fallback to QPS routing due to:")
            logger.info(traceback.format_exc())
        
        # 步驟5: Fallback routing !!!!!!!!!!!!!!!
        fallback_start = time.time()
        self.uncached_prefix_tokens = len(token_ids)
        result = self._fallback_routing(endpoints, request_stats, request)
        timing_data['fallback_time'] = time.time() - fallback_start
        
        # 計算總時間
        timing_data['total_time'] = time.time() - start_time
        
        # 更新全局時間追蹤
        request_timing_data = getattr(request.state, 'timing_data', None)
        if request_timing_data:
            timing_monitor.update_router_timing(request_timing_data, timing_data)
        
        # 移除舊的 CSV 寫入邏輯，現在使用統一的 RequestTimingMonitor
        
        return result

    def _find_best_matched(self, matched_infos):
        best_matched_info = None
        for instance_info in matched_infos:
            if best_matched_info is None or instance_info[1][-1][1] > best_matched_info[1][-1][1]:
                best_matched_info = instance_info
        if best_matched_info is None:
            raise ValueError("no best matched instance was found")
        return best_matched_info

    async def _find_best_ttft(self, endpoints, matched_infos, best_matched_info,
                              request_stats):
        matched_stats = []
        matched_urls = []
        for matched_info in matched_infos:
            url = await self._get_instance_url(endpoints, matched_info[0])
            stats = request_stats.get(url, None)
            if stats is None:
                raise ValueError(f"{url} provides no request stats ")
            if stats.uncomputed_prefix_tokens > 0 and stats.engine_prefill_tps <= 0:
                raise ValueError(f"{url} provides no way to forecasted queue time")
            matched_urls.append(url)
            matched_stats.append(stats)

        # cache matched pass
        best_ttft = float('inf')
        best_ttft_url = None
        for i, matched_info in enumerate(matched_infos):
            logger.debug(f"-------------- URL:{matched_urls[i]} --------------")
            ttft = self._estimate_ttft(matched_info, best_matched_info,
                                       matched_stats[i])
            if best_ttft_url is None or ttft <= best_ttft:
                best_ttft = ttft
                best_ttft_url = matched_urls[i]

        # cache not matched pass
        matched_url_set = set(matched_urls)
        not_matched_endpoints = [endpoint for endpoint in endpoints if endpoint.url not in matched_url_set]
        for endpoint in not_matched_endpoints:
            url = endpoint.url
            stats = request_stats.get(url, None)
            if stats is None:
                raise ValueError(f"{url} provides no request stats ")
            logger.debug(f"-------------- URL:{url} --------------")
            ttft = self._estimate_ttft(None, best_matched_info, stats)
            if best_ttft_url is None or ttft <= best_ttft:
                best_ttft = ttft
                best_ttft_url = url

        if best_ttft_url is None:
            raise ValueError(f"no best TTFT instance was found")
        return best_ttft_url

    def _estimate_ttft(self, matched_info, best_matched_info, stats):
        transfer_time = self._calc_transfer_time(matched_info, best_matched_info)
        # TODO take computation time of num_uncached_token into account
        if stats.uncomputed_prefix_tokens == 0:
            forecasted_queue_time = 0
        else:
            forecasted_queue_time = (stats.uncomputed_prefix_tokens /
                                     stats.engine_prefill_tps)
        ttft = forecasted_queue_time + transfer_time

        logger.debug(f"-------------- time estimations --------------")
        logger.debug(f"uncomputed_prefix_tokens: {stats.uncomputed_prefix_tokens}")
        logger.debug(f"engine_prefill_tps: {stats.engine_prefill_tps}")
        logger.debug(f"transfer_time: {transfer_time}")
        logger.debug(f"forecasted_queue_time: {forecasted_queue_time}")
        logger.debug(f"ttft: {ttft}")
        return ttft

    async def _get_instance_url(self, endpoints, instance_id):
        url = self.instance_id_to_url.get(instance_id, None)
        if url is not None:
            return url
        for endpoint in endpoints:
            msg = QueryInstMsg(
                event_id="",
                ip=endpoint.url.split(f":{endpoint.url.split(":")[-1]}")[
                    0
                ].split("//")[1]
            )
            ret_msg = await self.kv_manager.handle_orchestration_message(msg)
            self.instance_id_to_url[ret_msg.instance_id] = endpoint.url
            if ret_msg.instance_id == instance_id:
                url = endpoint.url
        if url is None:
            raise ValueError(f"cannot resolve URL for {instance_id}")
        return url

    def _calc_transfer_time(self, matched_info, best_matched_info):
        transfer_time = 0
        for chunk in best_matched_info[1]:
            if matched_info is not None and chunk[1] <= matched_info[1][-1][1]:
                continue
            # TODO better estimations
            if chunk[0] == "LocalCpuBackend":
                transfer_time += 0.01
            elif chunk[0] == "LocalDiskBackend":
                transfer_time += 0.015
            else:
                transfer_time += 0.01
        return transfer_time

    def _fallback_routing(self, endpoints, request_stats, request):
        session_id = request.headers.get(self.session_key, None)
        logger.debug(f"Got session id: {session_id}")

        # Update the hash ring with the current list of endpoints
        self._update_hash_ring(endpoints)

        if session_id is None:
            # Route base on QPS if no session ID is present
            url = self._qps_routing(endpoints, request_stats)
        else:
            # Use the hash ring to get the endpoint for the session ID
            url = self.hash_ring.get_node(session_id)
        return url


# Instead of managing a global _global_router, we can define the initialization functions as:
def initialize_routing_logic(
    routing_logic: RoutingLogic, *args, **kwargs
) -> RoutingInterface:
    if routing_logic == RoutingLogic.ROUND_ROBIN:
        logger.info("Initializing round-robin routing logic")
        return RoundRobinRouter()
    elif routing_logic == RoutingLogic.SESSION_BASED:
        logger.info(f"Initializing session-based routing logic with kwargs: {kwargs}")
        return SessionRouter(kwargs.get("session_key"))
    elif routing_logic == RoutingLogic.KVAWARE:
        logger.info("Initializing kvaware routing logic")
        router = KvawareRouter(
            kwargs.get("lmcache_controller_port"),
            kwargs.get("session_key"),
            kwargs.get("kv_aware_threshold"),
            kwargs.get("tokenizer"),
            kwargs.get("instance_id_to_url"),
        )
        router.start_kv_manager()
        return router
    elif routing_logic == RoutingLogic.PREFIXAWARE:
        logger.info("Initializing prefix-aware routing logic")
        return PrefixAwareRouter()
    elif routing_logic == RoutingLogic.DISAGGREGATED_PREFILL:
        logger.info("Initializing disaggregated prefill routing logic")
        return DisaggregatedPrefillRouter(
            kwargs.get("prefill_model_labels"), kwargs.get("decode_model_labels")
        )
    elif routing_logic == RoutingLogic.TTFT:
        logger.info("Initializing ttft routing logic")
        router = TtftRouter(
            kwargs.get("lmcache_controller_port"),
            kwargs.get("session_key"),
            kwargs.get("tokenizer"),
            kwargs.get("instance_id_to_url"),
        )
        router.start_kv_manager()
        return router
    else:
        raise ValueError(f"Invalid routing logic {routing_logic}")


def reconfigure_routing_logic(
    routing_logic: RoutingLogic, *args, **kwargs
) -> RoutingInterface:
    # Remove the existing routers from the singleton registry
    for cls in (
        SessionRouter,
        RoundRobinRouter,
        KvawareRouter,
        DisaggregatedPrefillRouter,
    ):
        if cls in SingletonABCMeta._instances:
            del SingletonABCMeta._instances[cls]
    return initialize_routing_logic(routing_logic, *args, **kwargs)


def get_routing_logic() -> RoutingInterface:
    # Look up in our singleton registry which router (if any) has been created.
    for cls in (
        SessionRouter,
        RoundRobinRouter,
        KvawareRouter,
        PrefixAwareRouter,
        DisaggregatedPrefillRouter,
        TtftRouter,
    ):
        if cls in SingletonABCMeta._instances:
            return cls()
    raise ValueError("The global router has not been initialized")
