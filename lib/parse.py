import re

def is_metadata(line):
    """
    Determines if a line is a metadata line.

    Args:
        line (str): The input line to check.

    Returns:
        bool: True if the line is metadata, False otherwise.

    Examples:
        >>> is_metadata("TK8707         A01        A01    1987  7 18  17 40  42 50.0 N 144 49.9 E")
        True
        >>> is_metadata("TK8707         A01                                                                        .00  11.8280  32.7850  11.8252")
        False
    """
    pattern_cruise = r"^[A-Z]{2}\d{4}"  # Example: TK8707
    pattern_station = r"A[0-2][0-9]"    # Example: A01
    pattern_year = r"\d{4}"             # Example: 1987
    pattern_month = r"[0-1]?[0-9]"      # Example: 7 or 07
    pattern_day = r"[0-3]?[0-9]"        # Example: 18
    pattern_hour = r"[0-2]?[0-9]"       # Example: 17
    pattern_minute = r"[0-6]?[0-9]"     # Example: 40
    pattern_lat = r"[0-1]?[0-8]?[0-9]\s+[0-6]?[0-9]\.[0-9]\s+[NS]"  # Example: 42 50.0 N
    pattern_lon = r"[0-1]?[0-8]?[0-9]\s+[0-6]?[0-9]\.[0-9]\s+[EW]"  # Example: 144 49.9 E

    # Combine all patterns with spaces in between
    pattern_metadata = (
        rf"{pattern_cruise}\s+{pattern_station}\s+{pattern_station}\s+"
        rf"{pattern_year}\s+{pattern_month}\s+{pattern_day}\s+"
        rf"{pattern_hour}\s+{pattern_minute}\s+{pattern_lat}\s+{pattern_lon}"
    )

    return bool(re.match(pattern_metadata, line))



if __name__ == "__main__":
    import doctest
    doctest.testmod()
