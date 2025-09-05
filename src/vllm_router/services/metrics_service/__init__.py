from prometheus_client import Gauge

# --- Prometheus Gauges ---
# Existing metrics
num_requests_running = Gauge(
    "vllm:num_requests_running", "Number of running requests", ["server"]
)
num_requests_waiting = Gauge(
    "vllm:num_requests_waiting", "Number of waiting requests", ["server"]
)
gpu_prefix_cache_hit_rate = Gauge(
    "vllm:gpu_prefix_cache_hit_rate",
    "GPU Prefix Cache Hit Rate",
    ["server"],
)
gpu_prefix_cache_hits_total = Gauge(
    "vllm:gpu_prefix_cache_hits_total",
    "Total GPU Prefix Cache Hits",
    ["server"],
)
gpu_prefix_cache_queries_total = Gauge(
    "vllm:gpu_prefix_cache_queries_total",
    "Total GPU Prefix Cache Queries",
    ["server"],
)
current_qps = Gauge("vllm:current_qps", "Current Queries Per Second", ["server"])
avg_decoding_length = Gauge(
    "vllm:avg_decoding_length", "Average Decoding Length", ["server"]
)
num_prefill_requests = Gauge(
    "vllm:num_prefill_requests", "Number of Prefill Requests", ["server"]
)
num_decoding_requests = Gauge(
    "vllm:num_decoding_requests", "Number of Decoding Requests", ["server"]
)

# New metrics per dashboard update
healthy_pods_total = Gauge(
    "vllm:healthy_pods_total", "Number of healthy vLLM pods", ["server"]
)
avg_latency = Gauge(
    "vllm:avg_latency", "Average end-to-end request latency", ["server"]
)
avg_itl = Gauge("vllm:avg_itl", "Average Inter-Token Latency", ["server"])
num_requests_swapped = Gauge(
    "vllm:num_requests_swapped", "Number of swapped requests", ["server"]
)

# KV Cache Transfer Metrics
kv_cache_transfer_time = Gauge(
    "vllm:kv_cache_transfer_time_ms", 
    "Time spent transferring KV cache between instances in milliseconds", 
    ["server", "transfer_type"]
)
kv_cache_transfer_count = Gauge(
    "vllm:kv_cache_transfer_count_total", 
    "Total number of KV cache transfers", 
    ["server", "transfer_type"]
)
kv_cache_transfer_throughput = Gauge(
    "vllm:kv_cache_transfer_throughput_gbps", 
    "KV cache transfer throughput in GB/s", 
    ["server", "transfer_type"]
)
kv_cache_transfer_size = Gauge(
    "vllm:kv_cache_transfer_size_bytes", 
    "Size of KV cache transfers in bytes", 
    ["server", "transfer_type"]
)