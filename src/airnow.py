"""
This module provides classes for retrieving and handling pollution data from 
the AirNow website.

Classes:
- _AirNow: A base class for interacting with the AirNow website and retrieving 
station information.
- Pollution: A class for retrieving and upserting pollution data into a database's 
"Pollution" table.

Usage:
1. Import the necessary classes:
    from airnow import _AirNow, Pollution

2. Create an instance of the Pollution class:
    pollution = Pollution()

3. Use the methods provided by the Pollution class to retrieve and upsert pollution 
data.
    pollution.upsert_data(year=1400, month=12)

Note:
- The module assumes the presence of a data_handler module for handling database
interactions.
- The code is designed to work with the AirNow website and the specific structure 
of its pages. Any changes to the website's structure may require modifications to 
the code.

For more information on how to use the classes and methods, refer to the inline
documentation and examples within the code.
"""

import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd

from .data_handler import DatabaseManager


class _AirNow:
    """A class for retrieving and managing air quality data from the AirNow website.

    This class provides methods for retrieving station information, pollution data,
    and upserting the data into a database's "Pollution" table.
    """

    _headers = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
            "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8,fa;q=0.7",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "http://airnow.tehran.ir",
        "Referer": "http://airnow.tehran.ir/",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ),
    }

    _data = {
        "ctl00$ContentPlaceHolder1$btnSearch": "  نمایش   ",
    }

    url = "http://airnow.tehran.ir/home/DataArchive.aspx"

    def __init__(self):
        self._first_response = self._setup_session()
        self.stations = self._find_options(self._first_response)
        self.stations_info = None

    def _setup_session(self):
        response = requests.get(self.url, timeout=100)
        sessionid = response.headers["Set-Cookie"].split(";")[0]
        self._headers["Cookie"] = sessionid
        self._update_hidden_inputs(response)
        return response

    def _update_hidden_inputs(self, response):
        soup = BeautifulSoup(response.content, "html.parser")
        hidden_inputs = soup.find_all("input", {"type": "hidden"})
        hidden_inputs = {inp["name"]: inp["value"] for inp in hidden_inputs}
        hidden_inputs.pop("delcfrm", None)
        hidden_inputs.pop("btnfrm", None)
        self._data.update(hidden_inputs)

    @staticmethod
    def get_station_ids() -> dict:
        """Retrieve the station IDs and names.

        Makes a request to the AirNow website to fetch the available station IDs
        and their corresponding names.

        Returns:
            dict: A dictionary containing the station IDs as keys and the
            corresponding names as values.

        Example:
            >>> station_ids = get_station_ids()
            >>> print(station_ids)
            {'1': 'Station 1', '2': 'Station 2', '3': 'Station 3', ...}

        """
        url = "http://airnow.tehran.ir/home/DataArchive.aspx"
        response = requests.get(url, timeout=100)
        station_ids = _AirNow._find_options(response)
        return station_ids

    @staticmethod
    def _find_options(response) -> dict:
        soup = BeautifulSoup(response.content, "html.parser")
        attr = {"id": "ContentPlaceHolder1_ddlStation"}
        options = soup.find("select", attr)
        assert isinstance(options, Tag)
        options = options.find_all("option")
        options = {option["value"]: option.text for option in options}
        return options

    @staticmethod
    def _extract_table(response) -> pd.DataFrame:
        soup = BeautifulSoup(response.content, "html.parser")
        table_html = soup.find("table")
        table = pd.DataFrame(
            [
                [cell.text for cell in row.find_all(["th", "td"])]
                for row in table_html.find_all("tr")  # type: ignore
            ]
        )
        return table

    def get_stations_info(self) -> pd.DataFrame:
        """Retrieve basic information about stations.

        Retrieves the date of establishment and district for each station from the specified URL
        and merges it with the station ID. Returns the resulting dataframe.

        Returns:
            pandas.DataFrame: Dataframe containing the station ID, station name, district,
            and date of establishment.
        """
        url = "http://airnow.tehran.ir/home/stationInfo.aspx"
        response = requests.get(url, timeout=100)
        table = self._extract_table(response)
        table[2] = table[2].str.extract(r"(\d{4}/\d{2}/\d{2})")
        table = table.iloc[1:]
        table.columns = ["Station", "District", "Date_of_Establishment"]
        station_ids = {
            station_name: int(station_id)
            for station_id, station_name in self.stations.items()
        }
        table.index = table["Station"].map(station_ids)  # type: ignore
        table.index.name = "ID"
        dbman = DatabaseManager()
        dbman.upsert_results("Stations", table)
        return table


class Pollution(_AirNow):
    """A subclass of _AirNow that specializes in retrieving and upserting pollution data.

    This class inherits all the methods from the _AirNow class and adds additional
    functionality for retrieving and upserting pollution data into a database's "Pollution" table.

    Example:
        pollution = Pollution()

        # Upsert combined pollution data for all stations on a specific day to database
        pollution.upsert_data(year=1401, month=6, day=23)

        # Retrieve data for all stations on a specific month
        data = pollution.get_data(year=1401, month=6)
        print(data)
    """

    url = "http://airnow.tehran.ir/home/DataArchive.aspx"

    def __init__(self):
        super().__init__()
        self._headers["Referer"] = "http://airnow.tehran.ir/home/DataArchive.aspx"

    def upsert_data(self, year=None, month=None, day=None, station=None) -> None:
        """Upsert pollution data into the database's "Pollution" table.

        Retrieves pollution data based on specified parameters (year, month, day,
        and station) and upserts the data into the "Pollution" table in the connected
        database.

        Args:
            year (int): The year of the desired data.
            month (int, optional): The month of the desired data. Defaults to None.
            day (int, optional): The day of the desired data. Defaults to None.
            station (str, optional): The station identifier. Defaults to None.

        Returns:
            None

        Note:
            - The method internally calls the `get_data` method to retrieve the
            pollution data.
            - The retrieved data is then passed to the `upsert_results` method of the
            connected database's "Pollution" table to upsert the data.
            - If no parameters are provided, the method retrieves the data for the
            previous day.
            - The connected database should have a "Pollution" table with an
            `upsert_results` method to handle the upsertion of the pollution data.

        Example:
            To upsert combined pollution data for all stations on a specific day
            into the "Pollution" table:
            >>> upsert_data(year=2023, month=6, day=23)
            To upsert pollution data for a specific station on a specific day into
            the "Pollution" table:
            >>> upsert_data(year=2023, month=6, day=23, station="ABC123")
        """
        data = self.get_data(year, month, day, station)
        dbman = DatabaseManager()
        dbman.upsert_results("Pollution", data)

    def get_data(self, year=None, month=None, day=None, station=None) -> pd.DataFrame:
        """Retrieve data based on specified parameters.

        Retrieves the hourly data based on the specified parameters and returns
        the data as a pandas DataFrame.

        Args:
            year (int): The year of the desired data.
            month (int, optional): The month of the desired data. Defaults to None.
            day (int, optional): The day of the desired data. Defaults to None.
            station (str, optional): The station identifier. Defaults to None.

        Returns:
            pandas.DataFrame or None: DataFrame containing the requested hourly data.
                Returns None if no data is available.

        Note:
            - If only the year parameter is provided, data for the entire year is returned.
            - If the year and month parameters are provided, data for the entire month is returned.
            - If the year, month, and day parameters are provided, data for the specific
            day is returned.
            - If all parameters are provided, data for the specified station and day is returned.
            - The resulting data is cleaned using the _clean_output_table method.

        Example:
            To retrieve combined data for all stations in the year 1402:
            >>> data = get_data_by_year(1402)
            To retrieve combined data for all stations in Ordibehesht 1402:
            >>> data = get_data_by_month(1402, 2)
            To retrieve data for all stations on Ordibehesht 1, 1402:
            >>> data = get_data_by_day(1402, 2, 1)
            To retrieve data for station "21" on Ordibehesht 1, 1402:
            >>> data = get_data_by_station(1402, 2, 1, 21)

        Cleaning Process:
            The retrieved data is cleaned using the _clean_output_table method,
            which performs the following operations:
            - Maps the station names to their corresponding station IDs.
            - Converts the 'Hour' column to an integer type.
            - Sets a custom index based on the date, hour, and station ID.
            - Reorders the columns to have 'Date', 'Hour', 'Station', and the remaining columns.
            - Replaces '\xa0' (non-breaking space) values with None.
            - Converts pollution measures into the float data type.

        """
        if month is None:
            data = self.get_data_by_year(year)
        elif day is None:
            data = self.get_data_by_month(year, month)
        elif station is None:
            data = self.get_data_by_day(year, month, day)
        else:
            data = self.get_data_by_station(year, month, day, station)

        data = self._clean_output_table(data)
        return data

    def get_data_by_year(self, year) -> pd.DataFrame | None:
        """Retrieve combined data for all stations in a specific year.

        Retrieves the hourly data for all stations throughout the specified year and returns
        the combined data as a pandas DataFrame.

        Args:
            year (int): The year of the desired data.

        Returns:
            pandas.DataFrame or None: DataFrame containing the combined hourly data for all
            stations in the specified year.
                Returns None if no data is available.

        Note:
            The function utilizes the get_data_by_month method to retrieve data for each month
            of the specified year.
            If no data is available for any month or day or station, None is returned.
            The resulting data is concatenated into a single DataFrame.

        Example:
            To retrieve combined data for all stations in the year 1402:
            >>> data = get_data_by_year(1402)
        """
        month_tables = []
        for month in range(1, 12 + 1):
            month_table = self.get_data_by_month(year, month)
            if month_table is None:
                continue
            month_tables.append(month_table)
        if len(month_tables) == 0:
            return None
        table = pd.concat(month_tables, ignore_index=True)
        return table

    def get_data_by_month(self, year, month) -> pd.DataFrame | None:
        """Retrieve combined data for all stations in a specific month.

        Retrieves the hourly data for all stations throughout the specified month and returns
        the combined data as a pandas DataFrame.

        Args:
            year (int): The year of the desired data.
            month (int): The month of the desired data.

        Returns:
            pandas.DataFrame or None: DataFrame containing the combined hourly data
            for all stations in the specified month.
                Returns None if no data is available.

        Note:
            The function utilizes the get_data_by_day method to retrieve data for
            each day of the specified month.
            The maximum number of days in a month is determined based on the
            specified year and month.
            If no data is available for any day or station, None is returned.
            The resulting data is concatenated into a single DataFrame.

        Example:
            To retrieve combined data for all stations in Ordibehesht 1402:
            >>> data = get_data_by_month(1402, 2)
        """
        if month <= 6:
            max_day = 31
        elif (month <= 11) or (year % 33 in [1, 5, 9, 13, 17, 22, 26, 30]):
            max_day = 30
        else:
            max_day = 29

        day_tables = []
        for day in range(1, max_day + 1):
            day_table = self.get_data_by_day(year, month, day)
            if day_table is None:
                continue
            day_tables.append(day_table)
        if len(day_tables) == 0:
            return None
        table = pd.concat(day_tables, ignore_index=True)
        return table

    def get_data_by_day(self, year, month, day) -> pd.DataFrame | None:
        """Retrieve data for all stations on a specific day.

        Retrieves hourly data for all stations on the specified day and returns
        the combined data as a pandas DataFrame.

        Args:
            year (int): The year of the desired data.
            month (int): The month of the desired data.
            day (int): The day of the desired data.

        Returns:
            pandas.DataFrame: Dataframe containing the combined hourly data for
            all stations on the specified day.

        Note:
            The function relies on the get_data_by_station and get_stations_info
            methods to retrieve the data for each station.
            Only stations with a date of establishment on or before the specified
            day will be considered.
            If no data is available for any station, None is returned.
            The resulting data is concatenated into a single DataFrame.

        Example:
            To retrieve data for all stations on Ordibehesht 1, 1402:
            >>> data = get_data_by_day(1402, 2, 1)
        """
        date = f"{year}/{str(month):0>2}/{str(day):0>2}"
        print(date, end="\r")
        if self.stations_info is None:
            self.stations_info = self.get_stations_info()
        filt = self.stations_info["Date_of_Establishment"] <= date
        available_stations = filt[filt].index.to_list()
        station_tables = []
        for station in available_stations:
            station_table = self.get_data_by_station(year, month, day, station)
            if station_table["Station"].iloc[0] == "رکوردی برای نمایش موجود نیست.":
                continue
            station_tables.append(station_table)
        if len(station_tables) == 0:
            return None
        table = pd.concat(station_tables, ignore_index=True)
        return table

    def get_data_by_station(self, year, month, day, station, **kwargs) -> pd.DataFrame:
        """Retrieve data for a specific station on a given date.

        Retrieves hourly data for the specified station on the specified date,
        and returns the data as a pandas DataFrame.

        Args:
            year (int): The year of the desired data.
            month (int): The month of the desired data.
            day (int): The day of the desired data.
            station (str): The station identifier.
            **kwargs: Additional keyword arguments for customization.

        Returns:
            pandas.DataFrame: Dataframe containing the hourly data for the
            specified station and date.

        Note:
            The data is retrieved from the configured URL using the _request_hourly_data method,
            then processed and cleaned using internal methods.

        Example:
            To retrieve data for station "21" on Ordibehesht 1, 1402:
            >>> data = get_data_by_station(1402, 2, 1, 21)
        """
        station = str(station)
        date = f"{year}/{str(month):0>2}/{str(day):0>2}"
        response = self._request_hourly_data(station, date, **kwargs)
        table = self._extract_table(response)
        table = self._clean_input_table(table)
        return table

    def _request_hourly_data(self, station, date, time_unit="hour", decimal_numbers=2):
        decimal_numbers = str(decimal_numbers)
        staion_date = {
            "ctl00$ContentPlaceHolder1$ddlStation": station,
            "ctl00$ContentPlaceHolder1$pddFrom": date,
            "ctl00$ContentPlaceHolder1$pddTo": date,
            "ctl00$ContentPlaceHolder1$ddlReportType": time_unit,
            "ctl00$ContentPlaceHolder1$txtNumber": decimal_numbers,
        }
        self._data.update(staion_date)
        response = requests.post(
            self.url, data=self._data, headers=self._headers, timeout=100
        )
        return response

    @staticmethod
    def _clean_input_table(table: pd.DataFrame):
        table.columns = table.iloc[0]
        columns_renamer = {
            "ایستگاه": "Station",
            "ساعت": "Hour",
            "تاریخ": "Date",
        }
        table = table.rename(columns=columns_renamer)
        table = table.iloc[1:]
        table = table.reset_index(drop=True)
        return table

    def _clean_output_table(self, table):
        station_ids = {
            station_name: int(station_id)
            for station_id, station_name in self.stations.items()
        }
        table["Station"] = table["Station"].map(station_ids)
        table["Hour"] = table["Hour"].astype("int")

        table.index = table.apply(
            lambda row: f"{row.Date.replace('/', '')}{row.Hour:0>2}{str(row.Station):0>3}",
            axis="columns",
        )
        table.index.name = "ID"
        table = table[["Date", "Hour", "Station"] +
                      table.columns[1:-2].to_list()]

        new_values = table[table.columns[3:]].copy()
        new_values = new_values.applymap(lambda cell: cell.replace("/", "."))
        new_values = new_values.replace({"\xa0": None})
        new_values = new_values.astype("float")
        table[table.columns[3:]] = new_values

        return table
