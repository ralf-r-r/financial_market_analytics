def month_to_quarter(month: int) -> str:
    """
    Converts the month number of a date to a string for the quarter e.g. Q4
    :param month: int, month of the date
    :return: str, the quarter of the year, e.g. Q2
    """
    quarter = (month - 1) // 3 + 1
    return "Q" + str(quarter)
