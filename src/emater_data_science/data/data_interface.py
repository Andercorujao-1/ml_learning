# src/emater_data_science/data/data_interface.py
from collections.abc import Callable
from datetime import date
from typing import Literal, Any, TypeVar
import polars as pl
from sqlalchemy.orm import DeclarativeBase

from emater_data_science.data.api_data.api_data_interface import ApiDataInterface
from emater_data_science.data.database_data.database_data_interface import DatabaseDataInterface

T = TypeVar("T", bound="DeclarativeBase")

class DataInterface:
    _instance = None

    def __new__(cls, *args, **kwargs) -> "DataInterface":
        if cls._instance is None:
            cls._instance = super(DataInterface, cls).__new__(cls)
            return cls._instance
        else:
            return cls._instance

    def __init__(self) -> None:
        if hasattr(self, "_initialized") and self._initialized:
            return
        # Build the tables mapping once during initialization.
        self.tablesMapping: dict[str, str] = self._buildTablesMapping()
        self._initialized = True

    @staticmethod
    def _buildTablesMapping() -> dict[str, str]:
        """
        Combine tables from all data sources.
        Each source returns a list of table names.
        The resulting mapping has the table name as key and the source as value.
        """
        mapping: dict[str, str] = {}
        sources = [
            ("api", ApiDataInterface().fGetTablesList()),         # returns List[str]
            ("disk", DatabaseDataInterface().fGetTablesList()),  # returns List[str]
        ]
        for source_name, table_list in sources:
            for tableName in table_list:
                if tableName in mapping:
                    raise ValueError(
                        f"Duplicate table name '{tableName}' found in sources: {mapping[tableName]} and {source_name}."
                    )
                mapping[tableName] = source_name
        return mapping

    def fFetchTable(
        self,        tableName: str,
        callback: Callable[[pl.DataFrame], None] | None,
        tableFilter: dict | None = None,
        dateColumn: str | None = None,
        startDate: date | None = None,
        endDate: date | None = None
    ) -> None:
        """
        Fetch the table from the appropriate data source (API or database).
        The result is returned asynchronously via the callback as a Polars DataFrame.

        :param tableName: Name of the table to fetch.
        :param callback: Function to be called with the resulting DataFrame.
        :param tableFilter: Optional dictionary to filter the table rows.
        """
        if tableName not in self.tablesMapping:
            raise ValueError(f"Table '{tableName}' not found.")

        source = self.tablesMapping[tableName]
        if source == "api":
            ApiDataInterface().fFetchTable(tableName, callback, tableFilter)
        elif source == "disk":
            DatabaseDataInterface().fFetchTable(tableName=tableName, callback=callback, tableFilter=tableFilter,
                                                dateColumn=dateColumn, startDate=startDate, endDate=endDate)
        else:
            raise ValueError(f"Unknown source '{source}' for table '{tableName}'.")

    def fStoreTable(
        self,
        model: type[T], data:  pl.DataFrame,
        storageTarget: Literal["disk", "api"] = "disk",
    ) -> None:
        """
        Store the table data into the appropriate data source.

        - If the table is new (not in the mapping), store it using the provided storageTarget and update the mapping.
        - If the table already exists in the mapping, store it using its mapped source.

        :param data: A list of ORM objects representing rows from one SQLAlchemy table.
        :param storageTarget: The target storage ("disk" or "server") for new tables.
        """
   

        # Derive the table name from the first element.
        tableName = model.__table__.name  # type: ignore[attr-defined]

        if tableName not in self.tablesMapping:
            # New table: store using the provided storageTarget.
            if storageTarget == "api":
                ApiDataInterface().fStoreTable(model=model,data=data)
                self.tablesMapping[tableName] = "api"
            elif storageTarget == "disk":
                DatabaseDataInterface().fStoreTable(model=model,data=data)
                self.tablesMapping[tableName] = "disk"
            else:
                raise ValueError(
                    f"Invalid storage target '{storageTarget}'. Must be 'disk' or 'server'."
                )
        else:
            # Table exists: store using its mapped source.
            existingSource = self.tablesMapping[tableName]
            if existingSource == "api":
                ApiDataInterface().fStoreTable(model=model,data=data)
            elif existingSource == "disk":
                DatabaseDataInterface().fStoreTable(model=model,data=data)
            else:
                raise ValueError(
                    f"Unknown source '{existingSource}' for table '{tableName}'."
                )

    def fQueueIsEmpty(self) -> bool:
        """
        Check if the queue is empty in the database.
        """
        return DatabaseDataInterface().fQueueIsEmpty()

    def fDeleteRowsFromTable(self, tableName: str, tableFilter: dict) -> None:
        """
        Delete rows from the table based on the provided filter, using the appropriate data source.
        The source is determined from the existing tables mapping.

        :param tableName: Name of the table from which rows will be deleted.
        :param tableFilter: Dictionary specifying the filter for row deletion.
        """
        if tableName not in self.tablesMapping:
            raise ValueError(f"Table '{tableName}' not found in tables mapping.")

        source = self.tablesMapping[tableName]
        if source == "api":
            ApiDataInterface().fDeleteRows(tableName, tableFilter)
        elif source == "disk":
            DatabaseDataInterface().fDeleteRows(tableName, tableFilter)
        else:
            raise ValueError(f"Unknown source '{source}' for table '{tableName}'.")

    def fShutdown(self) -> None:
        """
        Shutdown all data interfaces.
        """
        # Uncomment the following line if API shutdown is implemented:
        # ApiDataInterface().fShutdown()
        DatabaseDataInterface().fShutdown()

    def fAddLog(self, logTable: Any) -> None:
        
        """
        Add a log to the database.

        :param logTable: The log object or table to store.
        """
        DatabaseDataInterface().fAddLog(logTable=logTable)


# --- Example usage ---
if __name__ == "__main__":
    try:
        di = DataInterface()
        print("Available tables mapping:")
        print(di.tablesMapping)
    except Exception as e:
        print(f"Error building tables mapping: {e}")

    print("\nFetching 'Mock_Agricultural_Data':")
    try:
        def print_df(df: pl.DataFrame) -> None:
            print(df)

        di.fFetchTable("Mock_Agricultural_Data", callback=print_df)
    except Exception as e:
        print(f"Error fetching table: {e}")
