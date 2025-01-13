from python_estudo.data.api_data.api_data_interface import ApiDataInterface
from python_estudo.data.csv_data.csv_data_interface import CsvDataInterface
from python_estudo.data.database_data.database_data_interface import DatabaseDataInterface
from loguru import logger


class DataInterface:
    def __init__(self):
        self.tables = None  # To store table-to-source mappings

    def getTablesList(self):
        """
        Builds the table mapping by combining tables from all sources.
        Raises an error if duplicate table names are found.
        """
        sources = [
            ("csv", CsvDataInterface.csvTablesList),
            ("api", ApiDataInterface.apiTablesList),
            ("database", DatabaseDataInterface.databaseTablesList),
        ]

        tablesMapping = {}

        for sourceName, getTablesMethod in sources:
            source_tables = getTablesMethod()
            for tableName, tableInfo in source_tables.items():
                if tableName in tablesMapping:
                    existingSource = tablesMapping[tableName]["source"]
                    logger.error(f"Duplicate table name: {tableName} in {existingSource} and {sourceName}")
                    raise ValueError(f"Duplicate table name '{tableName}' found in sources: {existingSource} and {sourceName}.")
                tablesMapping[tableName] = tableInfo

        return tablesMapping

    def fetchTable(self, tableName,tableFilter):
        """
        Fetches a table from the correct source based on the table mapping.
        """
        if self.tables is None:
            self.tables = self.getTablesList()

        if tableName not in self.tables:
            logger.error(f"Table not found: {tableName}")
            raise ValueError(f"Table '{tableName}' not found in any source.")

        # Call the fetch method for the table
        fetchMethod = self.tables[tableName]["fetchMethod"]
        try:
            return fetchMethod(tableName, tableFilter)
        except Exception as e:
            logger.exception(f"Failed to fetch table '{tableName}': {e}")
            raise


# Example usage
if __name__ == "__main__":
    data_interface = DataInterface()

    # Fetch tables list
    print("Tables List:")
    try:
        tables = data_interface.getTablesList()
        print(tables)
    except Exception as e:
        print(f"Error fetching tables list: {e}")

    # Fetch a specific table
    print("\nFetching Mock_Agricultural_Data:")
    try:
        data = data_interface.fetchTable("Mock_Agricultural_Data")
        print(data)
    except Exception as e:
        print(f"Error fetching table: {e}")
