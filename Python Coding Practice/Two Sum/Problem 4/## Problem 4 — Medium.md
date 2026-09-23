# Problem 4 — Medium

## Problem Statement

You are processing a log file containing timestamps of system events. Each timestamp is an integer representing seconds since epoch. The log is already sorted in ascending order.

Find all gaps between consecutive events that exceed a threshold `max_gap`. Return a list of tuples, where each tuple contains `(start_timestamp, end_timestamp, gap_size)` for gaps larger than `max_gap`.

## Input / Function Requirements

```python
def find_large_gaps(timestamps: list[int], max_gap: int) -> list[tuple[int, int, int]]:
    """
    timestamps: sorted list of integers (seconds since epoch)
    max_gap: maximum acceptable gap between consecutive events
    Returns: list of (start_ts, end_ts, gap) for gaps > max_gap
    """
```

## Examples
```python 
find_large_gaps([100, 150, 200, 500, 600, 1000], 200)
# Returns: [(200, 500, 300), (600, 1000, 400)]

find_large_gaps([10, 20, 30, 40], 15)
# Returns: []

find_large_gaps([100, 500], 300)
# Returns: [(100, 500, 400)]

find_large_gaps([1000], 100)
# Returns: []
```

## Constraints
- 0 <= len(timestamps) <= 100,000
- timestamps is sorted in ascending order

- 0 <= timestamps[i] <= 1,000,000,000

- 0 <= max_gap <= 1,000,000

## Edge Cases
- Empty list

- Single element (no gaps possible)

- No gaps exceed threshold
- 
- All gaps exceed threshold
- 
- Exactly equal to max_gap (should NOT be included, only strictly greater)