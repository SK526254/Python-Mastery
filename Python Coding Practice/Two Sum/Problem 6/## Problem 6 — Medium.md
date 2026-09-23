# Problem 6 — Medium

## Problem Statement

You are processing a stream of sensor readings where each reading is a temperature value. The sensor occasionally produces erroneous spikes.

A valid temperature sequence is defined as one where no two consecutive readings differ by more than a threshold `max_diff`.

Given a list of readings, find the longest contiguous subarray that forms a valid temperature sequence. Return the start and end indices (0-based) of this subarray.

If multiple subarrays have the same maximum length, return the one with the smallest start index.

## Input / Function Requirements

```python
def find_longest_valid_sequence(readings: list[int], max_diff: int) -> tuple[int, int]:
    """
    readings: list of integer temperature readings
    max_diff: maximum acceptable difference between consecutive readings
    Returns: (start_index, end_index) of the longest valid contiguous subarray
             If the list is empty, return (-1, -1)
    """

```

## Examples
``` python

find_longest_valid_sequence([20, 22, 25, 24, 30, 32, 35, 80, 82, 84], 5)
# Returns: (0, 6) because [20, 22, 25, 24, 30, 32, 35] is valid (length 7)
#          [80, 82, 84] is also valid but shorter (length 3)

find_longest_valid_sequence([10, 15, 20, 30, 35], 10)
# Returns: (0, 4) because entire array is valid

find_longest_valid_sequence([10, 50, 100, 150], 20)
# Returns: (0, 0) because no two consecutive elements are within 20

find_longest_valid_sequence([], 10)
# Returns: (-1, -1)

find_longest_valid_sequence([5], 10)
# Returns: (0, 0)
```

## Constraints
- 0 <= len(readings) <= 100,000

- 0 <= readings[i] <= 1,000,000

- 0 <= max_diff <= 1,000,000

## Edge Cases
- Empty list

- Single element

- All elements form a valid sequence

- No two consecutive elements are valid

- Multiple sequences of the same maximum length

- Large list (performance matters)