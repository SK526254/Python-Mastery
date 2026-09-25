## Problem 8 — Hard

### Problem Statement

You are validating a batch of financial ledger entries. Each entry contains an account ID, a timestamp, and a signed amount:

```python
(account_id, timestamp, amount)
```

The entries are sorted first by `account_id` and then by `timestamp`.

For each account independently, find every pair of entries whose timestamps are at most
`window_seconds` apart and whose amounts sum to exactly zero.

Each entry may appear in at most one returned pair.

When several valid pairs are possible for an entry, match it with the earliest eligible unused entry for that account.

Return the matched pairs in the order in which the first entry of each pair appears in the input.

### Input / Function Requirements

```python
def find_reversing_entries(
    entries: list[tuple[str, int, int]],
    window_seconds: int,
) -> list[tuple[tuple[str, int, int], tuple[str, int, int]]]:
    """
    entries: sorted by account_id, then timestamp.
        Each entry is (account_id, timestamp, amount).

    window_seconds: non-negative maximum allowed timestamp difference.

    Returns:
        A list of matched entry pairs.

    Matching rules:
    - Both entries in a pair must have the same account_id.
    - Their absolute timestamp difference must be <= window_seconds.
    - Their amounts must sum to zero.
    - Each input entry can occur in at most one pair.
    - If multiple unused entries could match an entry, use the earliest
      eligible one for that account.
    - Pairs must be returned in the order that their first entry occurs
      in the input.
    """
```

### Examples

```python
entries = [
    ("A", 100, 500),
    ("A", 105, -500),
    ("A", 110, 200),
    ("A", 130, -200),
    ("B", 100, 300),
    ("B", 103, -300),
]

find_reversing_entries(entries, 10)
# Returns:
# [
#   (("A", 100, 500), ("A", 105, -500)),
#   (("B", 100, 300), ("B", 103, -300)),
# ]
# The A entries at timestamps 110 and 130 differ by 20, so they do not match.
```

```python
entries = [
    ("A", 100, 50),
    ("A", 101, -50),
    ("A", 102, -50),
    ("A", 103, 50),
]

find_reversing_entries(entries, 5)
# Returns:
# [
#   (("A", 100, 50), ("A", 101, -50)),
#   (("A", 102, -50), ("A", 103, 50)),
# ]
```

```python
entries = [
    ("A", 100, 100),
    ("A", 101, 200),
    ("A", 102, -100),
    ("A", 103, -200),
]

find_reversing_entries(entries, 5)
# Returns:
# [
#   (("A", 100, 100), ("A", 102, -100)),
#   (("A", 101, 200), ("A", 103, -200)),
# ]
```

```python
entries = []

find_reversing_entries(entries, 60)
# Returns: []
```

### Constraints

- `0 <= len(entries) <= 200,000`
- Entries are sorted by `account_id`, then by `timestamp`
- `0 <= timestamp <= 1_000_000_000`
- `-1_000_000_000 <= amount <= 1_000_000_000`
- `0 <= window_seconds <= 1_000_000`
- `account_id` is a non-empty string
- Multiple entries may have the same account ID, timestamp, and amount

### Edge Cases

- Empty input
- A single entry
- Entries from different accounts that otherwise look like a match
- Several possible opposite-amount matches
- Entries exactly `window_seconds` apart
- Entries one second outside the time window
- Multiple duplicate entries
- Zero amounts, where two zero-amount entries sum to zero
- A very large account group that must be processed efficiently