# Problem 3 — Medium

## Problem Statement

You are validating a data pipeline that processes financial transactions. Each transaction has an amount, and you need to identify if there exist two transactions (at different positions) whose amounts are close to each other within a certain tolerance.

Given a list of transaction amounts and a tolerance value `k`, find if there exist two indices `i` and `j` such that:

- `i != j`
- `abs(amounts[i] - amounts[j]) <= k`

Return `True` if such a pair exists, otherwise `False`.

## Input / Function Requirements

```python
def has_close_transactions(amounts: list[int], k: int) -> bool:
    """
    amounts: list of positive integers representing transaction amounts
    k: non-negative integer representing the tolerance
    Returns: True if two different positions have amounts within k of each other
    """
```

## Examples
```python 
has_close_transactions([10, 50, 100, 105, 200], 10)   # Returns: True (100 and 105 differ by 5)
has_close_transactions([10, 50, 100, 200, 300], 10)   # Returns: False
has_close_transactions([5, 5, 5, 5], 0)                # Returns: True (exact matches)
has_close_transactions([1, 100, 200], 50)              # Returns: False
```

## Constraints
- 2 <= len(amounts) <= 50,000
- 1 <= amounts[i] <= 1,000,000
- 0 <= k <= 1,000,000

## Edge Cases
- Exact matches (difference = 0)
- k = 0 (only exact matches count)
- All elements far apart
- Large list (performance matters)
- Minimum list size (2 elements)