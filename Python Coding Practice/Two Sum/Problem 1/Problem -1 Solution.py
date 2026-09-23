def find_transaction_pair(amounts: list[int], target: int) -> tuple[int, int]:
    """
    amounts: sorted list of positive integers
    target: integer representing the target sum
    Returns: tuple of two indices (i, j) where i < j
    """
    ## Solution - 1 : Using 
    i = 0 
    j = len(amounts) - 1

    while i < j:
        s = amounts[i] + amounts[j]
        if amounts[i] + amounts[j] == target:
            return (i,j)
        elif s < target:
            i += 1
        else:
            j -= 1
    
    # d = dict()
    # i = 0
    # l = len(amounts)

    # while i < l:
    #     if amounts[i] in d.keys():
    #         return ( d[amounts[i]],i)
    #     else:
    #         d[target - amounts[i]] = i
    #     i += 1

print(find_transaction_pair([10,25,30,50,60,80], 75)) 
print(find_transaction_pair(amounts = [40, 60], target = 100))         
print(find_transaction_pair(amounts = [5, 12, 19, 24, 33, 50], target = 55))         
print(find_transaction_pair(amounts = [10, 15, 40, 45, 80], target = 85))         
print(find_transaction_pair(amounts = [10, 20, 20, 35, 50], target = 40))         
print(find_transaction_pair(amounts = [5000, 10000, 500000, 1000000], target = 1500000))         
print(find_transaction_pair(amounts = [5, 8, 12, 15, 100, 120], target = 115))         
print(find_transaction_pair(amounts = [10, 20, 90, 150, 300, 500], target = 110))        
print(find_transaction_pair(amounts = [1, 2, 3, 4], target = 3))         

amounts = list(range(10, 20010, 2))

print(find_transaction_pair(amounts = amounts, target = 39998))         
