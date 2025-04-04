from emater_data_science.data.database_data.central_database_connection import (
    CentralDatabaseConnection,
)
from emater_data_science.data.database_data.database_logger_manager import (
    DatabaseLoggerManager,
)
import time

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

    def fStoreTable(self, model, data) -> None:
        CentralDatabaseConnection().fWrite(model=model, data=data)

    def fQueueIsEmpty(self) -> bool:
        return CentralDatabaseConnection().fQueueIsEmpty()

    def fFetchTable(self,        tableName: str,
        callback,
        tableFilter: dict | None = None,
        dateColumn: str | None = None,
        startDate= None,
        endDate = None) -> None:
        CentralDatabaseConnection().fRead(
            tableName=tableName, callback=callback, tableFilter=tableFilter, dateColumn=dateColumn, startDate=startDate, endDate=endDate
        )

    def fDeleteRows(self, table, tableFilter) -> None:
        CentralDatabaseConnection().fDeleteRows(table=table, tableFilter=tableFilter)

    def fShutdown(self) -> None:
        print("Shutting down DatabaseDataInterface...")
        time.sleep(0.2)
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
