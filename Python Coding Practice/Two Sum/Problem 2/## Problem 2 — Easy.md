# Problem 2 — Easy

## Problem Statement

You are processing a stream of transaction records where each record is a tuple `(transaction_id, amount)`. Some transactions are duplicates (same `transaction_id` appears multiple times).

Write a function that removes duplicates **in-place** from a sorted list of transactions, keeping only the **first occurrence** of each `transaction_id`. Return the new length of the deduplicated list.

The list is sorted by `transaction_id`, so duplicates are adjacent.

## Input / Function Requirements

```python
def deduplicate_transactions(transactions: list[tuple[str, float]]) -> int:
    """
    transactions: list of (transaction_id, amount) tuples, sorted by transaction_id
    Returns: new length after removing duplicates (modify list in-place)
    
    After the function runs, the first k elements should contain unique transaction_ids
    where k is the returned value.
    """
```
## Examples
```python
txns = [("A001", 100.0), ("A001", 105.0), ("A002", 200.0), ("A002", 195.0), ("A003", 300.0)]
deduplicate_transactions(txns)  # Returns: 3
# txns should now be: [("A001", 100.0), ("A002", 200.0), ("A003", 300.0), ...]

txns = [("B001", 50.0)]
deduplicate_transactions(txns)  # Returns: 1

txns = [("C001", 10.0), ("C001", 12.0), ("C001", 15.0)]
deduplicate_transactions(txns)  # Returns: 1
```

## Constraints
- 1 <= len(transactions) <= 100,000
- transactions is sorted by transaction_id
- transaction_id is a non-empty string
- amount is a positive float

## Edge Cases
- Single element list

