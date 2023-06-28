"""Command Line Interface
"""
import argparse

from src import Pollution, Ckan

def create_sql_filter(input_args):
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
parser.add_argument("-c", "--ckan", action="store_true", help="Upload data to Ckan", default=False)
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
            filt = create_sql_filter(args)
            ckan.add_recordes("Pollution", filt)
