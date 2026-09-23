def has_close_transactions(amounts: list[int], k: int) -> bool:
    """
    amounts: list of positive integers representing transaction amounts
    k: non-negative integer representing the tolerance
    Returns: True if two different positions have amounts within k of each other
    """

    amounts.sort()

    for i in range (1,len(amounts)-1):
        if amounts[i+1] - amounts[i] <= k:
            return True
        

    return False

# # If Sorting is not allowed
# def has_close_transactions(amounts: list[int], k: int) -> bool:
#     seen = set()
#     for amount in amounts:
#         # Check if any value in [amount - k, amount + k] exists
#         for val in range(amount - k, amount + k + 1):
#             if val in seen:
#                 return True
#         seen.add(amount)
#     return False



print(has_close_transactions([10, 50, 100, 105, 200], 5)  )
print(has_close_transactions([10, 50, 100, 200, 300], 10) )
print(has_close_transactions([5, 5, 5, 5], 0))
print(has_close_transactions([1, 100, 200,250], 50))
