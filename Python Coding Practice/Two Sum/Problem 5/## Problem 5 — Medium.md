# Problem 5 — Medium

## Problem Statement

You are building a data quality check for a financial reconciliation pipeline. Given two sorted lists of transaction IDs (one from your internal system, one from a bank statement), find all transaction IDs that appear in both lists (the intersection).

Return the intersection as a sorted list. Each transaction ID should appear only once in the result, even if it appears multiple times in both input lists.

## Input / Function Requirements

```python
def reconcile_transactions(internal: list[str], bank: list[str]) -> list[str]:
    """
    internal: sorted list of transaction IDs from internal system
    bank: sorted list of transaction IDs from bank statement
    Returns: sorted list of transaction IDs present in both lists (no duplicates)
    """
```

## Examples

```python
reconcile_transactions(["A001", "A002", "A003", "A005"], ["A002", "A003", "A004", "A006"])
# Returns: ["A002", "A003"]

reconcile_transactions(["X001", "X001", "X002"], ["X001", "X001", "X001", "X003"])
# Returns: ["X001"] (deduplicated)

reconcile_transactions(["A001", "A002"], ["B001", "B002"])
# Returns: []

reconcile_transactions(["A001"], ["A001", "A001"])
# Returns: ["A001"]
```

## Constraints
- 0 <= len(internal), len(bank) <= 100,000

- Both lists are sorted alphabetically

- Transaction IDs are non-empty strings

- Duplicates may exist in either list

## Edge Cases
- One or both lists empty

- No overlap

- Complete overlap

- Duplicates in both lists

- One list is much larger than the other

