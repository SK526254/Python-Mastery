def process_window(entries, windowseconds, i, j, resultset):


    chosenset = set()

    while i < j:
        if i in chosenset: 
            i += 1
            pass
        
        n = i + 1
        while n <= j:
            if (entries[n][1] - entries[i][1]) <= windowseconds and \
                (entries[i][2] + entries[n][2] == 0):
                resultset.append(
                    (entries[i], entries[n])
                )
                chosenset.add(n)
                break
            n += 1
        i += 1
            






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

    i, j = 0, 0
    resultset = []

    while j < len(entries):
        while j + 1 < len(entries) and entries[j + 1][0] == entries[i][0]:
            j += 1
        if j < len(entries): process_window(entries, window_seconds, i, j, resultset)

        i = j + 1
        j += 1

    process_window(entries, window_seconds, i, j-1, resultset)

    return resultset
  


entries = [
    ("A", 100, 500),
    ("A", 105, -500),
    ("A", 110, 200),
    ("A", 130, -200),
    ("B", 100, 300),
    ("B", 103, -300),
]

print(find_reversing_entries(entries, 10))
# Returns:
# [
#   (("A", 100, 500), ("A", 105, -500)),
#   (("B", 100, 300), ("B", 103, -300)),
# ]
# The A entries at timestamps 110 and 130 differ by 20, so they do not match.


entries = [
    ("A", 100, 50),
    ("A", 101, -50),
    ("A", 102, -50),
    ("A", 103, 50),
]

print(find_reversing_entries(entries, 5))
# Returns:
# [
#   (("A", 100, 50), ("A", 101, -50)),
#   (("A", 102, -50), ("A", 103, 50)),
# ]


entries = [
    ("A", 100, 100),
    ("A", 101, 200),
    ("A", 102, -100),
    ("A", 103, -200),
]

print(find_reversing_entries(entries, 5))
# Returns:
# [
#   (("A", 100, 100), ("A", 102, -100)),
#   (("A", 101, 200), ("A", 103, -200)),
# ]


entries = []

print(find_reversing_entries(entries, 60))
# Returns: []
