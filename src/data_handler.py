"""
This module provides classes for managing a SQLite database and interacting
with the CKAN API for data management.

Classes:
- DatabaseManager: Manages a SQLite database and performs operations such as 
  table creation, data insertion/updating, and data retrieval.
- Ckan: Interacts with the CKAN API for dataset creation, update, and resource management.

Usage:
1. Initialize an instance of DatabaseManager to manage the SQLite database.
2. Use the DatabaseManager instance to create tables, upsert data, and retrieve
   data from the database.
3. Initialize an instance of Ckan to interact with the CKAN API.
4. Use the Ckan instance to create datasets, update existing datasets, and manage 
   resources associated with the datasets.

Note:
Before using the CKAN functionality, make sure to provide the necessary information
in YAML files:
- "website_info.yaml": Contains the CKAN API address and access token.
- "metadata.yaml": Contains metadata for dataset creation, including resource 
  aliases and additional information.

Example:
# Database management
db_manager = DatabaseManager()
data = pd.DataFrame(...)  # Data to upsert
db_manager.upsert_results("Stations", data)
result = db_manager.get_data("Stations")

# CKAN interaction
ckan_client = Ckan()
ckan_client.create_dataset()
ckan_client.update_dataset()
ckan_client.create_resources()
ckan_client.create_resource("Stations")
data = pd.DataFrame(...)  # Data to upload
ckan_client.add_records("Stations", data)
"""
from json import JSONDecodeError
from pathlib import Path
import time

import requests
import sqlalchemy as sa
from tqdm import tqdm
import pandas as pd
import yaml


class DatabaseManager:
    """
    A class for managing a SQLite database and performing operations such as table creation,
    data insertion/updating, and data retrieval.
    """

    def __init__(self) -> None:
        self.engine = sa.create_engine("sqlite:///AirQuality.db")
        self.create_table = {
            "Stations": self.create_table_stations,
            "Pollution": self.create_table_pollution,
        }

    def is_table_in_database(self, table_name: str):
        """Checks if a table exists in the database.

        Args:
            table_name (str): Name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        metadata_obj = sa.MetaData()
        metadata_obj.reflect(self.engine)
        return table_name in metadata_obj.tables

    def create_table_stations(self):
        """Create Stations Table Schema

        Creates the schema for the Stations table, which stores information about
        air quality monitoring stations.
        The table contains the following columns:
        - ID: Unique identifier for the station (integer, primary key)
        - Station: Name of the station (string)
        - District: District where the station is located (integer)
        - Date_of_Establishment: Date of establishment of the station (string)
        """
        metadata_obj = sa.MetaData()
        sa.Table(
            "Stations",
            metadata_obj,
            sa.Column("ID", sa.Integer, primary_key=True),
            sa.Column("Station", sa.String),
            sa.Column("District", sa.Integer),
            sa.Column("Date_of_Establishment", sa.String),
        )
        metadata_obj.create_all(self.engine)

    def create_table_pollution(self):
        """Create Pollution Table Schema

        Creates the schema for the Pollution table, which stores air pollution data.
        The table contains the following columns:
        - ID: Unique identifier for the data entry (string, primary key)
        - Date: Date of the pollution data (string)
        - Hour: Hour of the pollution data (integer)
        - Station: ID of the monitoring station (integer, foreign key)
        - CO: Carbon monoxide level (float)
        - O3: Ozone level (float)
        - NO: Nitric oxide level (float)
        - NO2: Nitrogen dioxide level (float)
        - NOx: Nitrogen oxides level (float)
        - SO2: Sulfur dioxide level (float)
        - PM10: Particulate matter (PM10) level (float)
        - PM2.5: Particulate matter (PM2.5) level (float)
        """
        metadata_obj = sa.MetaData()
        sa.Table(
            "Pollution",
            metadata_obj,
            sa.Column("ID", sa.String, primary_key=True),
            sa.Column("Date", sa.String),
            sa.Column("Hour", sa.Integer),
            sa.Column(
                "Station", sa.Integer, sa.ForeignKey("Stations.ID"), nullable=False
            ),
            sa.Column("CO", sa.Float),
            sa.Column("O3", sa.Float),
            sa.Column("NO", sa.Float),
            sa.Column("NO2", sa.Float),
            sa.Column("NOx", sa.Float),
            sa.Column("SO2", sa.Float),
            sa.Column("PM10", sa.Float),
            sa.Column("PM2.5", sa.Float),
        )
        metadata_obj.create_all(self.engine)

    def upsert_results(self, table_name: str, table: pd.DataFrame):
        """Upsert Data into the Database

        Inserts or updates data into the specified table.

        If the table doesn't exist, it creates the table schema.
        It first deletes existing rows in the table that have the same IDs as the input data,
        and then inserts the new data into the table.

        Args:
            table_name (str): Name of the table.
            table (pd.DataFrame): Data to be upserted, stored as a Pandas DataFrame.
        """
        if not self.is_table_in_database(table_name):
            self.create_table[table_name]()
        with self.engine.connect() as connection:
            ids = str(table.index.to_list()).replace(
                "[", "(").replace("]", ")")
            connection.execute(
                sa.text(f"DELETE FROM {table_name} WHERE ID IN {ids}"))
            connection.commit()  # type: ignore
        table.to_sql(table_name, self.engine,
                     if_exists="append", chunksize=10_000)

    def get_data(self, table_name: str, filters: str | None = None) -> pd.DataFrame:
        """
        Retrieve data from the specified table in the database.

        Args:
            table_name (str): Name of the table from which to retrieve the data.
            filters (str | None, optional): Optional filters to apply to the query.
                The filters should be provided as a valid SQL WHERE clause. Defaults to None.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the retrieved data.

        Raises:
            sa.exc.ArgumentError: If the specified table does not exist in the database.

        Example:
            # Retrieve all data from the "Stations" table
            pollution_data = db_manager.get_data("Stations")

            # Retrieve data from the "Pollution" table with filters
            filters = "ID LIKE '140201__001%'"
            stations_data = db_manager.get_data("Pollution", filters)
        """
        sql_statement = f"SELECT * FROM {table_name}"
        if filters is not None:
            sql_statement += f" WHERE {filters}"
        sql_statement = sa.text(sql_statement)
        with self.engine.connect() as connection:
            table = pd.read_sql(sql_statement, connection)
        return table


class Ckan:
    """Class for interacting with the CKAN API."""

    database_name = "AirQuality.db"

    def __init__(self):
        """Initialize the CKAN API client.

        Reads the necessary information from YAML files and sets up the API URL
        and header for making requests.
        """
        with open("website_info.yaml", encoding="utf-8") as yaml_file:
            website_info = yaml.load(yaml_file, yaml.CLoader)
        self.api_url = f"{website_info['address']}/api/3/action"
        self.header = {"X-CKAN-API-Key": website_info["token"]}

        with open(
            Path().joinpath("website", "metadata.yaml"), encoding="utf-8"
        ) as yaml_file:
            self.website_metadata = yaml.load(yaml_file, yaml.CLoader)

        with open(
            Path().joinpath("website", "dataset_notes.md"), encoding="utf-8"
        ) as text_file:
            self.website_metadata["dataset_metadata"]["notes"] = text_file.read(
            )

    def create_dataset(self):
        """Create a dataset using the provided metadata.

        Returns:
            dict: The JSON response from the API call.
        """
        response = requests.post(
            url=f"{self.api_url}/package_create",
            headers=self.header,
            json=self.website_metadata["dataset_metadata"],
            timeout=100,
        )
        return response.json()

    def update_dataset(self):
        """Update an existing dataset using the provided metadata.

        Returns:
            dict: The JSON response from the API call.
        """
        response = requests.post(
            url=f"{self.api_url}/package_update",
            headers=self.header,
            json=self.website_metadata["dataset_metadata"],
            timeout=100,
        )
        return response.json()

    def create_resources(self) -> None:
        """Create resources based on the website metadata.

        This function iterates over the website metadata and calls the `create_resource` method
        to create each individual resource.
        """
        for resource_metadata in self.website_metadata["resources"].keys():
            self.create_resource(resource_metadata)

    def create_resource(self, table_name: str) -> dict:
        """Create a resource with the specified table name.

        Args:
            table_name (str): The name of the table/resource to create.

        Returns:
            dict: The JSON response from the API call.
        """
        resource_metadata = self.website_metadata["resources"][table_name]
        response = requests.post(
            url=f"{self.api_url}/datastore_create",
            headers=self.header,
            json=resource_metadata,
            timeout=100,
        )
        return response.json()

    def delete_resource(self, resource_id: str) -> dict:
        """Delete a resource with the specified resource ID.

        Args:
            resource_id (str): The ID of the resource to delete.

        Returns:
            dict: The JSON response from the API call.
        """
        response = requests.post(
            url=f"{self.api_url}/resource_delete",
            headers=self.header,
            json={"id": resource_id},
            timeout=100,
        )
        return response.json()

    def find_resource_id(self, aliases):
        """Find the resource ID based on the provided aliases.

        Args:
            aliases: The aliases to search for.

        Returns:
            str: The resource ID if found, None otherwise.
        """
        response = requests.post(
            url=f"{self.api_url}/datastore_info",
            headers=self.header,
            json={"id": aliases},
            timeout=100,
        )
        try:
            dataset_id = response.json()["result"]["meta"]["id"]
        except KeyError:
            dataset_id = None
        return dataset_id

    def list_datastore_resources(self) -> list:
        """List the available datastore resources.

        Returns:
            list: The list of available resources.
        """
        response = requests.post(
            url=f"{self.api_url}/datastore_search",
            headers=self.header,
            json={"id": "_table_metadata"},
            timeout=100,
        )
        return response.json()

    def add_recordes(self, table_name: str, filters: str | None = None):
        """Add records to the specified resource.

        Args:
            table_name (str): The name of the resource to add records to.
            filters (str | None): Optional filters to apply when retrieving records.
            Defaults to None.
        """
        dbman = DatabaseManager()
        table = dbman.get_data(table_name, filters)
        records = table.astype(str).to_dict("records")
        self._upload_records(table_name, records)

    def _upload_records(self, table_name, records, chunk_size=500):
        """Upload records in chunks to the specified table/resource.

        Args:
            table_name (str): The name of the table/resource to upload records to.
            records: The records to upload.
            chunk_size (int): The size of each chunk. Defaults to 500.
        """
        left_bound = 0
        right_bound = min(chunk_size, len(records))
        pbar = tqdm(
            desc="Uploading Records",
            total=len(records),
            unit="records",
            unit_scale=True,
        )
        while left_bound != right_bound:
            report_chunk = records[left_bound:right_bound]
            pbar.update(right_bound - left_bound)
            record_count = right_bound == len(records)
            left_bound = right_bound
            right_bound = min(right_bound + chunk_size, len(records))
            while True:
                try:
                    self._upload_record_chunk(
                        table_name, report_chunk, record_count)
                    break
                except JSONDecodeError:
                    pass
                except ConnectionError:
                    time.sleep(60)
        pbar.close()

    def _upload_record_chunk(
        self, table_name: str, records: list[dict], record_count: bool = False
    ) -> None:
        """Upload a chunk of records to the specified table/resource.

        Args:
            table_name (str): The name of the table/resource to upload records to.
            records: The chunk of records to upload.
            record_count (bool): Whether to calculate the record count. Defaults to False.
        """
        alias = self.website_metadata["resources"][table_name]["aliases"][0]
        resource_id = self.find_resource_id(alias)
        assert resource_id is not None
        response = requests.post(
            url=f"{self.api_url}/datastore_upsert",
            headers=self.header,
            json={
                "resource_id": resource_id,
                "records": records,
                "calculate_record_count": str(record_count),
            },
            timeout=100,
        )
        assert response.json()["success"] is True
