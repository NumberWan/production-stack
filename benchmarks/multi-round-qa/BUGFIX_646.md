# Bug Fix for Issue #646: DataFrame Length Mismatch in multi-round-qa Benchmark

## Problem Description

**Issue**: [GitHub Issue #646](https://github.com/vllm-project/production-stack/issues/646)

When running the multi-round-qa benchmark, the program crashes with a `ValueError: Length of values (2) does not match length of index (1)` error when creating a DataFrame in the `UserSession.summary()` method.

## Root Cause Analysis

The issue occurs due to a race condition in the multi-threaded environment:

1. **Async Callbacks**: The `_update_result()` method is called from async callbacks running in different threads
2. **Concurrent Access**: The `summary()` method is called from the main thread while async callbacks are still updating the data lists
3. **Inconsistent State**: Without proper synchronization, different lists (`self.ttfts`, `self.prompt_lengths`, etc.) can have different lengths at the time of DataFrame creation

## Solution

### Changes Made

1. **Added Threading Import**: Added `import threading` to the imports
2. **Added Thread Lock**: Added `self._data_lock = threading.Lock()` in `UserSession.__init__()`
3. **Protected Data Updates**: Wrapped `_update_result()` method with `with self._data_lock:`
4. **Protected Data Access**: Wrapped `summary()` method with `with self._data_lock:` and added length consistency checks

### Code Changes

#### 1. Import Addition
```python
import threading
```

#### 2. UserSession.__init__() Method
```python
# Thread lock to ensure data consistency
self._data_lock = threading.Lock()
```

#### 3. _update_result() Method
```python
def _update_result(self, response: Response):
    with self._data_lock:
        self.prompt_lengths.append(response.prompt_tokens)
        self.generation_lengths.append(response.generation_tokens)
        self.ttfts.append(response.ttft)
        self.generation_times.append(response.generation_time)
        self.launch_times.append(response.launch_time)
        self.finish_times.append(response.finish_time)
```

#### 4. summary() Method
```python
def summary(self) -> pd.DataFrame:
    with self._data_lock:
        # Ensure all lists have the same length
        min_length = min(
            len(self.prompt_lengths),
            len(self.generation_lengths),
            len(self.ttfts),
            len(self.generation_times),
            len(self.launch_times),
            len(self.finish_times)
        )
        
        # Truncate all lists to the minimum length to ensure consistency
        prompt_lengths = self.prompt_lengths[:min_length]
        generation_lengths = self.generation_lengths[:min_length]
        ttfts = self.ttfts[:min_length]
        generation_times = self.generation_times[:min_length]
        launch_times = self.launch_times[:min_length]
        finish_times = self.finish_times[:min_length]
    
    df = pd.DataFrame()
    df["prompt_tokens"] = prompt_lengths
    df["generation_tokens"] = generation_lengths
    df["ttft"] = ttfts
    df["generation_time"] = generation_times
    df["user_id"] = self.user_config.user_id
    df["question_id"] = range(1, min_length + 1)
    df["launch_time"] = launch_times
    df["finish_time"] = finish_times
    return df
```

## Benefits

1. **Thread Safety**: All data access is now properly synchronized
2. **Data Consistency**: Ensures all DataFrame columns have the same length
3. **Error Prevention**: Eliminates the `ValueError` that was causing crashes
4. **Backward Compatibility**: No changes to the public API
5. **Performance**: Minimal overhead from the lock mechanism

## Testing

The fix has been tested with a mock implementation that simulates the race condition:
- ✅ Concurrent data updates from multiple threads
- ✅ Multiple calls to `summary()` during updates
- ✅ DataFrame creation with consistent column lengths
- ✅ No exceptions or crashes

## Files Modified

- `benchmarks/multi-round-qa/multi-round-qa.py`

## Author

@NumberWan - Fix for [GitHub Issue #646](https://github.com/vllm-project/production-stack/issues/646) 