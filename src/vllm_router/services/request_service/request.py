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

# --- Request Processing & Routing ---
import json
import os
import time
import uuid

import aiohttp
from fastapi import BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from requests import JSONDecodeError

from vllm_router.log import init_logger
from vllm_router.monitoring.request_timing import get_request_timing_monitor
from vllm_router.routers.routing_logic import (
    DisaggregatedPrefillRouter,
    KvawareRouter,
    PrefixAwareRouter,
    RoundRobinRouter,
    TtftRouter,
)
from vllm_router.service_discovery import get_service_discovery
from vllm_router.services.request_service.rewriter import (
    get_request_rewriter,
    is_request_rewriter_initialized,
)
from vllm_router.utils import replace_model_in_request_body, update_content_length

try:
    # Semantic cache integration
    from vllm_router.experimental.semantic_cache_integration import (
        store_in_semantic_cache,
    )

    semantic_cache_available = True
except ImportError:
    semantic_cache_available = False


logger = init_logger(__name__)


# TODO: (Brian) check if request is json beforehand
async def process_request(
    request: Request,
    body,
    backend_url,
    request_id,
    endpoint,
    background_tasks: BackgroundTasks,
    debug_request=None,
    uncached_prefix_tokens=None,
):
    """
    Process a request by sending it to the chosen backend.

    Args:
        request(Request): Request object.
        body: The content of the request to send to the backend.
        backend_url: The URL of the backend to send the request to.
        request_id: A unique identifier for the request.
        endpoint: The endpoint to send the request to on the backend.
        debug_request: The original request object from the client, used for
            optional debug logging.
        uncached_prefix_tokens: The number of uncached prefix tokens.
    Yields:
        The response headers and status code, followed by the response content.

    Raises:
        HTTPError: If the backend returns a 4xx or 5xx status code.
    """
    # 獲取時間追蹤監控器
    timing_monitor = get_request_timing_monitor()
    timing_data = getattr(request.state, 'timing_data', None)
    
    first_token = False
    total_len = 0
    start_time = time.time()
    
    # 記錄後端連接開始時間
    if timing_data:
        timing_monitor.record_backend_processing_start(timing_data)
    
    # 確保相對時間基準存在
    if timing_data and getattr(timing_data, '_start_epoch', 0) == 0:
        timing_data._start_epoch = start_time

    request.app.state.request_stats_monitor.on_new_request(
        backend_url, request_id, start_time, uncached_prefix_tokens
    )
    # Check if this is a streaming request
    try:
        request_json = json.loads(body)
        is_streaming = request_json.get("stream", False)
    except JSONDecodeError:
        # If we can't parse the body as JSON, assume it's not streaming
        raise HTTPException(status=400, detail="Request body is not JSON parsable.")

    # For non-streaming requests, collect the full response to cache it properly
    full_response = bytearray()

    # 記錄後端連接時間
    backend_connection_start = time.time()
    async with request.app.state.aiohttp_client_wrapper().request(
        method=request.method,
        url=backend_url + endpoint,
        headers=dict(request.headers),
        data=body,
        timeout=aiohttp.ClientTimeout(total=None),
    ) as backend_response:
        status_code = backend_response.status
        backend_connection_time = time.time() - backend_connection_start
        if timing_data:
            # 只輸出簡化欄位需要：backend_connection_time
            timing_monitor.record_step_time(timing_data, 'backend_connection_time', backend_connection_time)
        
        # Yield headers and status code first.
        yield backend_response.headers, backend_response.status
        # Stream response content.
        async for chunk in backend_response.content.iter_any():
            total_len += len(chunk)
            if not first_token:
                first_token = True
                if timing_data:
                    # 記錄首 token（相對請求開始）及將此刻視為 decode 開始
                    timing_monitor.record_first_token(timing_data)
                    timing_monitor.record_response_streaming_start(timing_data)
                request.app.state.request_stats_monitor.on_request_response(
                    backend_url, request_id, time.time()
                )
            # For non-streaming requests, collect the full response
            if full_response is not None:
                full_response.extend(chunk)
            yield chunk
        
        # 記錄最後一個 token 時間（並結束 decode 計時）
        if timing_data:
            timing_monitor.record_last_token(timing_data)
            timing_monitor.record_response_streaming_end(timing_data)

    # 記錄後端處理結束時間
    if timing_data:
        timing_monitor.record_backend_processing_end(timing_data)
        timing_data.total_tokens = total_len
        timing_data.is_streaming = is_streaming
    
    request.app.state.request_stats_monitor.on_request_complete(
        backend_url, request_id, time.time()
    )

    # if debug_request:
    #    logger.debug(f"Finished the request with request id: {debug_request.headers.get('x-request-id', None)} at {time.time()}")
    # Store in semantic cache if applicable
    # Use the full response for non-streaming requests, or the last chunk for streaming
    if request.app.state.semantic_cache_available:
        cache_chunk = bytes(full_response) if not is_streaming else chunk
        await store_in_semantic_cache(
            endpoint=endpoint, method=request.method, body=body, chunk=cache_chunk
        )
    if background_tasks and getattr(request.app.state, "callbacks", None):
        background_tasks.add_task(
            request.app.state.callbacks.post_request, request, full_response
        )

    # 在此統一完成計時（含 ttft / decode_time）
    if timing_data:
        try:
            timing_monitor.complete_request(
                timing_data,
                server_url=backend_url,
                status_code=int(status_code) if 'status_code' in locals() else 200,
            )
        except Exception:
            pass


async def route_general_request(
    request: Request, endpoint: str, background_tasks: BackgroundTasks
):
    """
    Route the incoming request to the backend server and stream the response back to the client.

    This function extracts the requested model from the request body and retrieves the
    corresponding endpoints. It uses routing logic to determine the best server URL to handle
    the request, then streams the request to that server. If the requested model is not available,
    it returns an error response.

    Args:
        request (Request): The incoming HTTP request.
        endpoint (str): The endpoint to which the request should be routed.

    Returns:
        StreamingResponse: A response object that streams data from the backend server to the client.
    """
    # 獲取時間追蹤監控器
    timing_monitor = get_request_timing_monitor()
    
    if isinstance(request.app.state.router, DisaggregatedPrefillRouter):
        response = await route_disaggregated_prefill_request(
            request, endpoint, background_tasks
        )
        return response
    
    in_router_time = time.time()
    # Same as vllm, Get request_id from X-Request-Id header if available
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    
    # 記錄請求解析開始時間
    request_parsing_start = time.time()
    request_body = await request.body()
    request_json = json.loads(request_body)
    request_parsing_time = time.time() - request_parsing_start
    
    # 獲取或創建 timing_data
    timing_data = getattr(request.state, 'timing_data', None)
    if timing_data:
        timing_monitor.record_step_time(timing_data, 'request_parsing_time', request_parsing_time)

    if request.query_params:
        request_endpoint = request.query_params.get("id")
    else:
        request_endpoint = None

    if getattr(request.app.state, "callbacks", None) and (
        response_overwrite := request.app.state.callbacks.pre_request(
            request, request_body, request_json
        )
    ):
        response_overwrite.headers["X-Request-Id"] = request_id
        return response_overwrite

    # 記錄模型驗證時間
    model_validation_start = time.time()
    requested_model = request_json.get("model", None)
    if requested_model is None:
        if timing_data:
            timing_monitor.complete_request(timing_data, status_code=400, error_message="Missing model")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid request: missing 'model' in request body."},
            headers={"X-Request-Id": request_id},
        )
    model_validation_time = time.time() - model_validation_start
    
    if timing_data:
        timing_monitor.record_step_time(timing_data, 'model_validation_time', model_validation_time)

    # Apply request rewriting if enabled
    if is_request_rewriter_initialized():
        rewriter = get_request_rewriter()
        rewritten_body = rewriter.rewrite_request(
            request_body, requested_model, endpoint
        )
        logger.info(f"Request for model {requested_model} was rewritten")
        request_body = rewritten_body
        # Update request_json if the body was rewritten
        try:
            request_json = json.loads(request_body)
        except JSONDecodeError:
            logger.warning("Failed to parse rewritten request body as JSON")
            raise HTTPException(
                status_code=400, detail="Request body is not JSON parsable."
            )

    # 記錄端點發現時間
    endpoint_discovery_start = time.time()
    service_discovery = get_service_discovery()
    endpoints = service_discovery.get_endpoint_info()
    endpoint_discovery_time = time.time() - endpoint_discovery_start
    
    if timing_data:
        timing_monitor.record_step_time(timing_data, 'endpoint_discovery_time', endpoint_discovery_time)

    aliases = getattr(service_discovery, "aliases", None)
    if aliases and requested_model in aliases.keys():
        requested_model = aliases[requested_model]
        request_body = replace_model_in_request_body(request_json, requested_model)
        update_content_length(request, request_body)

    if not request_endpoint:
        endpoints = list(
            filter(
                lambda x: requested_model in x.model_names and not x.sleep,
                endpoints,
            )
        )
        engine_stats = request.app.state.engine_stats_scraper.get_engine_stats()
        request_stats = request.app.state.request_stats_monitor.get_request_stats(
            time.time(),
            [endpoint.url for endpoint in endpoints],
        )
    else:
        endpoints = list(
            filter(
                lambda x: requested_model in x.model_names
                and x.Id == request_endpoint
                and not x.sleep,
                endpoints,
            )
        )

    if not endpoints:
        return JSONResponse(
            status_code=400,
            content={
                "error": f"Model {requested_model} not found or vLLM engine is sleeping."
            },
        )

    logger.debug(f"Routing request {request_id} for model: {requested_model}")
    
    # 記錄路由決策時間
    routing_decision_start = time.time()
    
    # 為 Round Robin 路由執行非侵入式 LMCache lookup（在路由選擇前）
    if isinstance(request.app.state.router, RoundRobinRouter):
        try:
            # Only attempt when controller port is available
            lmcache_port = getattr(request.app.state, 'lmcache_controller_port', None)
            if lmcache_port is not None and timing_data is not None:
                # Lazy import to avoid hard dependency
                from lmcache.v1.cache_controller import controller_manager  # type: ignore
                from lmcache.v1.cache_controller.message import LookupMsg  # type: ignore
                from transformers import AutoTokenizer  # type: ignore
                from vllm_router.routers.routing_logic import extract_prompt

                # Build a minimal prompt from request body
                token_ids = []
                try:
                    prompt = extract_prompt(request_json)
                    # Use first endpoint's model as tokenizer source
                    if endpoints and endpoints[0].model_names:
                        tokenizer = AutoTokenizer.from_pretrained(endpoints[0].model_names[0])
                        token_ids = tokenizer.encode(prompt)
                except Exception as e:
                    # Debug: log the exception to understand why lookup fails
                    import logging
                    logging.getLogger(__name__).debug(f"RR lookup failed to get tokens: {e}")
                    token_ids = []

                if token_ids:
                    kv_mgr = controller_manager.LMCacheControllerManager(f"0.0.0.0:{lmcache_port}")
                    msg = LookupMsg(event_id="", tokens=token_ids)
                    # This call is sync in KvawareRouter via await; here we provide a sync handle
                    # The manager internally handles the orchestration; if it fails, we ignore.
                    res = kv_mgr.handle_orchestration_message(msg)

                    matched_tokens = 0
                    try:
                        if res and getattr(res, 'layout_info', None):
                            # Pick max matched tokens across instances
                            matched_tokens = max(v[1] for v in res.layout_info.values())
                    except Exception:
                        matched_tokens = 0

                    # Write into timing data for CSV
                    timing_data.matched_kvcache_tokens = int(matched_tokens)
                    timing_data.request_tokens = int(len(token_ids))
                    # 估算：未命中部分可能需傳輸的 token 數（監控用，不影響路由）
                    uncached_tokens = max(0, int(len(token_ids)) - int(matched_tokens))
                    if uncached_tokens > 0:
                        timing_data.kv_cache_transfer_count += 1
                        timing_data.kv_cache_transfer_tokens += int(uncached_tokens)
                    # We cannot precisely time lookup here; set to 0 if unknown
                    timing_monitor.record_kv_cache_lookup(
                        timing_data,
                        lookup_time=0.0,
                        hit=bool(matched_tokens > 0),
                    )
                    
                    # Debug: log the values being set
                    import logging
                    logging.getLogger(__name__).info(f"RR lookup: tokens={len(token_ids)}, matched={matched_tokens}, hit={bool(matched_tokens > 0)}")
        except Exception as e:
            # Debug: log the exception to understand why lookup fails
            import logging
            logging.getLogger(__name__).debug(f"RR lookup exception: {e}")
            # Never break RR routing due to monitoring
            pass
    
    if request_endpoint:
        server_url = endpoints[0].url
        logger.debug(
            f"Routing request {request_id} to engine with Id: {endpoints[0].Id}"
        )

    elif isinstance(request.app.state.router, (KvawareRouter, PrefixAwareRouter, TtftRouter)):
        server_url = await request.app.state.router.route_request(
            endpoints, engine_stats, request_stats, request, request_json
        )
    else:
        server_url = request.app.state.router.route_request(
            endpoints, engine_stats, request_stats, request
        )
    routing_decision_time = time.time() - routing_decision_start
    
    if timing_data:
        # 路由決策耗時
        timing_monitor.record_step_time(timing_data, 'routing_decision_time', routing_decision_time)
        # 記錄路由器類型（簡化輸出用）
        router_cls = type(request.app.state.router).__name__
        router_map = {
            'KvawareRouter': 'kvaware',
            'PrefixAwareRouter': 'prefixaware',
            'TtftRouter': 'ttft',
            'RoundRobinRouter': 'roundrobin',
            'SessionRouter': 'session',
        }
        timing_data.routing_logic = router_map.get(router_cls, router_cls.lower())

    curr_time = time.time()
    # Extract actual session ID from request headers for logging
    session_key = (
        getattr(request.app.state.router, "session_key", None)
        if hasattr(request.app.state.router, "session_key")
        else None
    )
    session_id = (
        request.headers.get(session_key, None) if session_key is not None else None
    )
    session_id_display = session_id if session_id is not None else "None"

    uncached_prefix_tokens = getattr(request.app.state.router, "uncached_prefix_tokens", None)

    # Debug logging to help troubleshoot session ID extraction
    logger.debug(
        f"Debug session extraction - Router type: {type(request.app.state.router).__name__}"
    )
    logger.debug(f"Debug session extraction - Session key config: {session_key}")
    logger.debug(f"Debug session extraction - Request headers: {dict(request.headers)}")
    logger.debug(f"Debug session extraction - Extracted session ID: {session_id}")

    logger.info(
        f"Routing request {request_id} with session id {session_id_display} to {server_url} at {curr_time}, process time = {curr_time - in_router_time:.4f}"
    )
    stream_generator = process_request(
        request,
        request_body,
        server_url,
        request_id,
        endpoint,
        background_tasks,
        uncached_prefix_tokens=uncached_prefix_tokens,
    )
    headers, status = await anext(stream_generator)
    headers_dict = {key: value for key, value in headers.items()}
    headers_dict["X-Request-Id"] = request_id
    return StreamingResponse(
        stream_generator,
        status_code=status,
        headers=headers_dict,
        media_type="text/event-stream",
    )


async def send_request_to_prefiller(
    client: aiohttp.ClientSession, endpoint: str, req_data: dict, request_id: str
):
    """
    Send a request to a prefiller service.
    """
    req_data = req_data.copy()
    req_data["max_tokens"] = 1
    if "max_completion_tokens" in req_data:
        req_data["max_completion_tokens"] = 1

    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id,
    }

    async with client.post(endpoint, json=req_data, headers=headers) as response:
        response.raise_for_status()
        return await response.json()


async def send_request_to_decode(
    client: aiohttp.ClientSession, endpoint: str, req_data: dict, request_id: str
):
    """
    Asynchronously stream the response from a service using a persistent client.
    """
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id,
    }

    async with client.post(endpoint, json=req_data, headers=headers) as response:
        response.raise_for_status()
        async for chunk in response.content.iter_any():
            yield chunk


async def route_disaggregated_prefill_request(
    request: Request,
    endpoint: str,
    background_tasks: BackgroundTasks,
):
    in_router_time = time.time()
    # Same as vllm, Get request_id from X-Request-Id header if available
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    request_json = await request.json()

    orig_max_tokens = request_json.get("max_tokens", 0)
    request_json["max_tokens"] = 1
    st = time.time()
    try:
        await send_request_to_prefiller(
            request.app.state.prefill_client, endpoint, request_json, request_id
        )
        et = time.time()
        logger.info(f"{request_id} prefill time (TTFT): {et - st:.4f}")
        logger.info(
            f"Routing request {request_id} with session id None to {request.app.state.prefill_client._base_url} at {et}, process time = {et - in_router_time:.4f}"
        )
        request_json["max_tokens"] = orig_max_tokens
    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP error in prefiller: {e}", exc_info=True)
        return JSONResponse(
            status_code=e.status,
            content={
                "error": {
                    "message": f"Prefiller error: {e.message}",
                    "type": "prefiller_error",
                    "code": e.status,
                }
            },
            headers={"X-Request-Id": request_id},
        )
    except Exception as e:
        logger.error(f"Unexpected error in prefiller: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "message": f"Prefiller error: {str(e)}",
                    "type": "prefiller_error",
                    "code": 500,
                }
            },
            headers={"X-Request-Id": request_id},
        )

    async def generate_stream():
        try:
            async for chunk in send_request_to_decode(
                request.app.state.decode_client, endpoint, request_json, request_id
            ):
                yield chunk
        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error in decoder: {e}", exc_info=True)
            try:
                error_text = e.message
            except Exception:
                error_text = f"HTTP {e.status}"
            # Yield error as JSON response
            error_response = {
                "error": {
                    "message": f"Decoder error: {error_text}",
                    "type": "decoder_error",
                    "code": e.status,
                }
            }
            yield json.dumps(error_response).encode("utf-8")
        except Exception as e:
            logger.error(f"Unexpected error in decoder: {e}", exc_info=True)
            # Yield error as JSON response
            error_response = {
                "error": {
                    "message": f"Decoder error: {str(e)}",
                    "type": "decoder_error",
                    "code": 500,
                }
            }
            yield json.dumps(error_response).encode("utf-8")

    curr_time = time.time()
    logger.info(
        f"Routing request {request_id} with session id None to {request.app.state.decode_client._base_url} at {curr_time}, process time = {curr_time - et:.4f}"
    )

    return StreamingResponse(
        generate_stream(),
        media_type="application/json",
        headers={"X-Request-Id": request_id},
    )


async def route_sleep_wakeup_request(
    request: Request,
    endpoint: str,
    background_tasks: BackgroundTasks,
):
    in_router_time = time.time()
    # Same as vllm, Get request_id from X-Request-Id header if available
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())

    if request.query_params:
        request_endpoint = request.query_params.get("id")
    else:
        request_endpoint = None

    if request_endpoint is None:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid request: missing target Engine Id."},
            headers={"X-Request-Id": request_id},
        )

    service_discovery = get_service_discovery()
    endpoints = service_discovery.get_endpoint_info()

    endpoints = list(filter(lambda x: x.Id == request_endpoint, endpoints))
    if not endpoints:
        return JSONResponse(
            status_code=400,
            content={"error": f"Engine with Id {request_endpoint} not found."},
        )
    logger.debug(f"Routing request {request_id} to engine with Id: {endpoints[0].Id}")

    server_url = endpoints[0].url
    curr_time = time.time()
    logger.info(
        f"Routing request {request_id} to {server_url} at {curr_time}, process time = {curr_time - in_router_time:.4f}"
    )

    headers = {
        "X-Request-Id": request_id,
    }

    if VLLM_API_KEY := os.getenv("VLLM_API_KEY"):
        logger.info("Using vllm server authentication")
        headers["Authorization"] = f"Bearer {VLLM_API_KEY}"

    url = server_url + endpoint

    async with aiohttp.ClientSession() as client:
        if endpoint == "/is_sleeping":
            async with client.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        else:
            request_body = await request.body()
            response_status = None
            if request_body:
                req_data = json.loads(request_body)
                async with client.post(url, json=req_data, headers=headers) as response:
                    response.raise_for_status()
                    response_status = response.status
            else:
                async with client.post(url, headers=headers) as response:
                    response.raise_for_status()
                    response_status = response.status

            pod_name = endpoints[0].pod_name
            if endpoint == "/sleep":
                service_discovery.add_sleep_label(pod_name)
            elif endpoint == "/wake_up":
                service_discovery.remove_sleep_label(pod_name)

            return JSONResponse(
                status_code=response_status,
                content={"status": "success"},
                headers={"X-Request-Id": request_id},
            )
