from typing import Literal
from emater_data_science.logging.logging_table_model import LoggingTable, LoggingTableModel

AllowedLogLevels = Literal["ERROR", "userAction", "executionState"]

class LogInDisk:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(LogInDisk, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True
        self._fSetStorageFunction()

    def _fSetStorageFunction(self) -> None:
        from emater_data_science.data.data_interface import DataInterface
        self.storageFunction = DataInterface().fAddLog

    def log(self, level: AllowedLogLevels, message: str, variablesJson: str = "") -> None:
        loggingTableModel = LoggingTableModel(
            level=level, message=message, variablesJson=variablesJson
        )
        loggingTable = LoggingTable.fromModel(loggingTableModel)
        self.storageFunction(logTable=loggingTable)
        
