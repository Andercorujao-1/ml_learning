from emater_data_science.data.database_data.central_database_connection import (
    CentralDatabaseConnection,
)
from emater_data_science.data.database_data.database_logger_manager import (
    DatabaseLoggerManager,
)


class DatabaseDataInterface:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DatabaseDataInterface, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._initialized = True

    def fGetTablesList(self) -> list[str]:
        return CentralDatabaseConnection().fListTables()

    def fStoreTable(self, data) -> None:
        CentralDatabaseConnection().fWrite(data=data)

    def fFetchTable(self, tableName, callback, tableFilter) -> None:
        CentralDatabaseConnection().fRead(
            tableName=tableName, callback=callback, tableFilter=tableFilter 
        )

    def fDeleteRows(self, table, tableFilter) -> None:
        CentralDatabaseConnection().fDeleteRows(table=table, tableFilter=tableFilter)

    def fShutdown(self) -> None:
        DatabaseLoggerManager().fShutdown()
        CentralDatabaseConnection().fShutdown()

    def fAddLog(self, logTable) -> None:
        DatabaseLoggerManager().fStoreLog(log=logTable)


# --- Example usage ---
if __name__ == "__main__":
    try:
        dbInterface = DatabaseDataInterface()
        print("Available tables:")
        print(dbInterface.fGetTablesList())
    except Exception as e:
        print(f"Error getting tables: {e}")

    # Example shutdown call
    try:
        dbInterface.fShutdown()
    except Exception as e:
        print(f"Error during shutdown: {e}")
