def reconcile_transactions(internal: list[str], bank: list[str]) -> list[str]:
    """
    internal: sorted list of transaction IDs from internal system
    bank: sorted list of transaction IDs from bank statement
    Returns: sorted list of transaction IDs present in both lists (no duplicates)
    """

    # internal_tnxs = set(internal)
    # bank_tnxs = set(bank)

    # common_tnxs = internal_tnxs.intersection(bank_tnxs)

    # common = list(common_tnxs)
    # common.sort()

    # return common


    result = []
    i,b = 0,0

    while i < len(internal) and b < len(bank):

        if i > 0 and internal[i] == internal[i-1]:
            i += 1
            continue
        if b > 0 and bank[b] == bank[b-1]:
            b += 1
            continue

        if internal[i] == bank[b]:
            result.append(internal[i])
            i += 1
            b += 1 
        elif internal[i] < bank[b]:
            i += 1
        else:
            b += 1
    return result
            


print(reconcile_transactions(["A001", "A002", "A003", "A005"], ["A002", "A003", "A004", "A006"]))
# Returns: ["A002", "A003"]

print(reconcile_transactions(["X001", "X001", "X002"], ["X001", "X001", "X001", "X003"]))
# Returns: ["X001"] (deduplicated)

print(reconcile_transactions(["A001", "A002"], ["B001", "B002"]))
# Returns: []

print(reconcile_transactions(["A001"], ["A001", "A001"]))
# Returns: ["A001"]
