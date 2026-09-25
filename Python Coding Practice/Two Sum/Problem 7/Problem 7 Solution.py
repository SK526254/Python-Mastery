def match_events(
    internal_events: list[tuple[str, int, int]],
    external_events: list[tuple[str, int, int]],
    tolerance_seconds: int,
) -> list[tuple[str, str]]:
    """
    internal_events: sorted by timestamp; each item is
        (internal_event_id, timestamp, amount)

    external_events: sorted by timestamp; each item is
        (external_event_id, timestamp, amount)

    tolerance_seconds: non-negative timestamp tolerance

    Returns:
        A list of (internal_event_id, external_event_id) pairs.

    Matching rules:
    - Absolute timestamp difference must be <= tolerance_seconds.
    - Amounts must match exactly.
    - Each event may participate in only one match.
    - For each internal event, select the earliest eligible unused
      external event.
    """

    result_set = []

    i = 0 
    j = 0 

    while i < len(internal) and j < len(external):
        # both timestamps under the threshold
            # both amounts matching 
                # then put those events in the results and increment both pointers and continue to the next pass
            # both amounts are not matching
                # the below conditions will be applied
        # both timestamps are not under the threshold
            # while is lesser
                # if the internal timestamp is lesser
                # if the external timestamp is lesser
        
        if abs(internal[i][1] - external[j][1]) <= tolerance_seconds:
            if internal[i][2] == external[j][2]:
                result_set.append((internal[i][0], external[j][0]))
                i += 1 
                j += 1
                continue

        if internal[i][1] <= external[j][1]:
            i += 1
        else: 
            j += 1

    return result_set







internal = [
    ("I1", 100, 50),
    ("I2", 105, 75),
    ("I3", 120, 50),
]

external = [
    ("E1", 98, 50),
    ("E2", 103, 75),
    ("E3", 110, 75),
    ("E4", 122, 50),
]

print(match_events(internal, external, 3))
# Returns: [("I1", "E1"), ("I2", "E2"), ("I3", "E4")]


internal = [
    ("I1", 100, 200),
    ("I2", 101, 200),
]

external = [
    ("E1", 100, 200),
]

print(match_events(internal, external, 2))
# Returns: [("I1", "E1")]
# E1 cannot be reused for I2.


internal = [
    ("I1", 100, 50),
    ("I2", 200, 75),
]

external = [
    ("E1", 95, 99),
    ("E2", 105, 50),
    ("E3", 210, 75),
]

print(match_events(internal, external, 10))
# Returns: [("I1", "E2"), ("I2", "E3")]


internal = []
external = [("E1", 100, 50)]

print(match_events(internal, external, 10))
# Returns: []
