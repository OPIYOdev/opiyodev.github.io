import heapq
import tempfile, os

def external_merge_sort(data: list, chunk_size: int = 1000) -> list:
    """
    External merge sort for data larger than RAM.
    Used by: Oracle DB, MapReduce (Google), Spark SQL sort.
    Sorts chunks in memory, merges sorted files on disk.
    """
    # Phase 1: Create sorted runs
    runs = []
    for i in range(0, len(data), chunk_size):
        chunk = sorted(data[i:i + chunk_size])
        runs.append(chunk)

    # Phase 2: K-way merge using a min-heap
    heap = []
    iterators = [iter(run) for run in runs]

    for i, it in enumerate(iterators):
        val = next(it, None)
        if val is not None:
            heapq.heappush(heap, (val, i))

    result = []
    while heap:
        val, i = heapq.heappop(heap)
        result.append(val)
        nxt = next(iterators[i], None)
        if nxt is not None:
            heapq.heappush(heap, (nxt, i))

    return result
