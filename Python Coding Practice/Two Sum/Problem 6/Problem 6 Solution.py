def find_longest_valid_sequence(readings: list[int], max_diff: int) -> tuple[int, int]:
    """
    readings: list of integer temperature readings
    max_diff: maximum acceptable difference between consecutive readings
    Returns: (start_index, end_index) of the longest valid contiguous subarray
             If the list is empty, return (-1, -1)
    """

    start_index , end_index = -1, -1

    greatest_length = 0
    check_lenth = 0
    for i in range(1,len(readings)):
        if readings[i] - readings[i-1] > max_diff:



find_longest_valid_sequence([20, 22, 25, 24, 30, 32, 35, 80, 82, 84], 5)
# Returns: (0, 6) because [20, 22, 25, 24, 30, 32, 35] is valid (length 7)
#          [80, 82, 84] is also valid but shorter (length 3)

find_longest_valid_sequence([10, 15, 20, 30, 35], 10)
# Returns: (0, 4) because entire array is valid

find_longest_valid_sequence([10, 50, 100, 150], 20)
# Returns: (0, 0) because no two consecutive elements are within 20

find_longest_valid_sequence([], 10)
# Returns: (-1, -1)

find_longest_valid_sequence([5], 10)
# Returns: (0, 0)
