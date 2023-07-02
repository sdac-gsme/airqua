"""Command Line Interface
"""
import argparse

from src import Pollution, Ckan


def build_id_filter_query(input_args):
    """Create a SQL filter condition based on the input arguments.

    Args:
        input_args (object): An object containing the input arguments.
            - year (int): The year value.
            - month (int or None): The month value (optional).
            - day (int or None): The day value (optional).
            - station (int or None): The station value (optional).

    Returns:
        str: The SQL filter condition as a string. The condition is constructed
            based on the provided input arguments and follows the pattern:
            "ID LIKE '{date_station}%'". Here, 'date_station' is a string formed
            by concatenating the year, month, day, and station values in the format:
            'YYYYMMDDSSS', where 'YYYY' represents the year, 'MM' represents the month
            (padded with leading zeros if provided), 'DD' represents the day
            (padded with leading zeros if provided), and 'SSS' represents the station
            (padded with leading zeros if provided).

    Example:
        >>> args = {
        ...     'year': 1401,
        ...     'month': 6,
        ...     'day': 15,
        ...     'station': 023
        ... }
        >>> build_id_filter_query(args)
        "ID LIKE '14010615023%'"
    """
    date_station = str(input_args.year)
    if input_args.month is not None:
        date_station += f"{input_args.month:0>2}"
    else:
        date_station += "__"
    if input_args.day is not None:
        date_station += f"{input_args.day:0>2}"
    else:
        date_station += "__"
    if input_args.station is not None:
        date_station += f"{input_args.station:0>3}"
    else:
        date_station += "___"

    return f"ID LIKE '{date_station}%'"


pollution = Pollution()

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["pollution", "stations"])
parser.add_argument(
    "-c", "--ckan", action="store_true", help="Upload data to Ckan", default=False
)
parser.add_argument("-y", "--year", default=None)
parser.add_argument("-m", "--month", default=None)
parser.add_argument("-d", "--day", default=None)
parser.add_argument("-s", "--station", default=None)

args = parser.parse_args()

if __name__ == "__main__":
    if args.command == "pollution":
        pollution.upsert_data(
            year=args.year,
            month=args.month,
            day=args.day,
            station=args.station,
        )
        if args.ckan:
            ckan = Ckan()
            filt = build_id_filter_query(args)
            ckan.add_recordes_from_database("Pollution", filt)
