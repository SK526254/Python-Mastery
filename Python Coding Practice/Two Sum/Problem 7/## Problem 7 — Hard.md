## Problem 7 — Hard

### Problem Statement

You are reconciling two sorted event streams from separate systems:

- `internal_events`: events recorded by your internal platform
- `external_events`: events recorded by an external provider

Each event is represented as:

```python
(event_id, timestamp, amount)
```

The lists are sorted in ascending order by `timestamp`.

An internal event can match an external event when both conditions hold:

1. Their timestamps differ by at most `tolerance_seconds`.
2. Their amounts are exactly equal.

Each event can be used in at most one match.

For every internal event, match it with the earliest eligible unused external event. Return all matched pairs in the order of the internal events.

Events that cannot be matched should not appear in the result.

### Input / Function Requirements

```python
def match_events(
    internal_events: list[tuple[str, int, int]],
    external_events: list[tuple[str, int, int]],
    tolerance_seconds: int,
) -> list[tuple[str, str]]:
    """
    internal_events: sorted by timestamp; each item is
        (internal_event_id, timestamp, amount)

    external_events: sorted by timestamp; each item is
        (external_event_id, timestamp, amount)

    tolerance_seconds: non-negative timestamp tolerance

    Returns:
        A list of (internal_event_id, external_event_id) pairs.

    Matching rules:
    - Absolute timestamp difference must be <= tolerance_seconds.
    - Amounts must match exactly.
    - Each event may participate in only one match.
    - For each internal event, select the earliest eligible unused
      external event.
    """
```

### Examples

```python
internal = [
    ("I1", 100, 50),
    ("I2", 105, 75),
    ("I3", 120, 50),
]

external = [
    ("E1", 98, 50),
    ("E2", 103, 75),
    ("E3", 110, 75),
    ("E4", 122, 50),
]

match_events(internal, external, 3)
# Returns: [("I1", "E1"), ("I2", "E2"), ("I3", "E4")]
```

```python
internal = [
    ("I1", 100, 200),
    ("I2", 101, 200),
]

external = [
    ("E1", 100, 200),
]

match_events(internal, external, 2)
# Returns: [("I1", "E1")]
# E1 cannot be reused for I2.
```

```python
internal = [
    ("I1", 100, 50),
    ("I2", 200, 75),
]

external = [
    ("E1", 95, 99),
    ("E2", 105, 50),
    ("E3", 210, 75),
]

match_events(internal, external, 10)
# Returns: [("I1", "E2"), ("I2", "E3")]
```

```python
internal = []
external = [("E1", 100, 50)]

match_events(internal, external, 10)
# Returns: []
```

### Constraints

- `0 <= len(internal_events), len(external_events) <= 100,000`
- Both input lists are sorted by timestamp in ascending order.
- `0 <= timestamp <= 1_000_000_000`
- `0 <= amount <= 1_000_000_000`
- `0 <= tolerance_seconds <= 1_000_000`
- Event IDs are non-empty strings.
- Multiple events may have identical timestamps and/or amounts.

### Edge Cases

- Either input list is empty
- No events match
- Several external events are eligible for an internal event
- Multiple events have the same timestamp and amount
- An external event matches the timestamp tolerance but has a different amount
- An external event is too early or too late
- One stream is substantially larger than the other
- An event that could match more than one event must only be used once