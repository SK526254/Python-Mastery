def find_large_gaps(timestamps: list[int], max_gap: int) -> list[tuple[int, int, int]]:
    """
    timestamps: sorted list of integers (seconds since epoch)
    max_gap: maximum acceptable gap between consecutive events
    Returns: list of (start_ts, end_ts, gap) for gaps > max_gap
    """

    result = []
    for i in range (1, len(timestamps)):
        gap = timestamps[i] - timestamps[i - 1]
        if gap > max_gap:
            result.append((timestamps[i-1], timestamps[i], gap))

    return result



print(find_large_gaps([100, 150, 200, 500, 600, 1000], 200))
# Returns: [(200, 500, 300), (600, 1000, 400)]

print(find_large_gaps([10, 20, 30, 40], 15))
# Returns: []

print(find_large_gaps([100, 500], 300))
# Returns: [(100, 500, 400)]

print(find_large_gaps([1000], 100))
# Returns: []
