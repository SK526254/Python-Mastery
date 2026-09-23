def deduplicate_transactions(transactions: list[tuple[str, float]]) -> int:
    """
    transactions: list of (transaction_id, amount) tuples, sorted by transaction_id
    Returns: new length after removing duplicates (modify list in-place)
    
    After the function runs, the first k elements should contain unique transaction_ids
    where k is the returned value.
    """
    ## Solution - 1
    # i = 1
    # last_unique_trxn = txns[0][0]
    # while i < len(txns):
    #     if last_unique_trxn == txns[i][0]:
    #         txns.pop(i)
    #     else:
    #         last_unique_trxn = txns[i][0]
    #         i += 1

    ## Solution - 2

    # i = 1
    # j = 0
    # while i < len(txns):
    #     if txns[j][0] == txns[i][0]:
    #         txns.pop(i)
    #     else:
    #         j = i
    #         i += 1
        

    # print(len(txns))

    ## Solution - 3

    if not transactions:
        return 0
    
    write = 1  # Position to write next unique element
    for read in range(1, len(transactions)):
        if transactions[read][0] != transactions[read - 1][0]:
            transactions[write] = transactions[read]
            write += 1
    return write




txns = [("A001", 100.0), ("A001", 105.0), ("A002", 200.0),("A002", 200.0), ("A002", 200.0),("A002", 200.0),("A002", 195.0), ("A003", 300.0)]
print(deduplicate_transactions(txns)) # Returns: 3
# txns should now be: [("A001", 100.0), ("A002", 200.0), ("A003", 300.0), ...]

txns = [("B001", 50.0)]
print(deduplicate_transactions(txns))  # Returns: 1

txns = [("C001", 10.0), ("C001", 12.0), ("C001", 15.0)]
print(deduplicate_transactions(txns))  # Returns: 1