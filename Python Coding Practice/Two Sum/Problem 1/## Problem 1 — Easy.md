## Problem 1 — Easy

### Problem Statement
You are given a sorted list of integers representing transaction amounts. Find two transactions whose sum equals a specified target amount. Return the indices of these two transactions (0-based).

Assume exactly one valid pair exists.

### Input / Function Requirements
```python
def find_transaction_pair(amounts: list[int], target: int) -> tuple[int, int]:
    """
    amounts: sorted list of positive integers
    target: integer representing the target sum
    Returns: tuple of two indices (i, j) where i < j
    """
```

### Examples
```python
find_transaction_pair(, 75)  # Returns: (1, 3) because 25 + 45 = 75[10][25][30][45][60][80]
find_transaction_pair(, 25)            # Returns: (0, 3) because 5 + 20 = 25[5][10][15][20]
```

### Constraints
- 2 <= len(amounts) <= 10,000
- amounts is sorted in ascending order
- 1 <= amounts[i] <= 1,000,000
- Exactly one valid pair exists

### Edge Cases
- Minimum list size (2 elements)
- Target formed by first and last elements
- Large values in the list