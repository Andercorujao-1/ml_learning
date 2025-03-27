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
        self.storageFunction = None
        self._is_shutdown = False  # Flag to indicate if logging is shut down.
        self._initialized = True
        self.log(
            level="executionState",
            message="LogInDisk::__init__ - Logger initialized.",
            variablesJson=""
        )

    def set_storage_function(self) -> None:
        # Import here to break any circular dependency.
        from emater_data_science.data.data_interface import DataInterface
        self.storageFunction = DataInterface.fAddLog
        self.log(
            level="executionState",
            message="LogInDisk::set_storage_function - Storage function set.",
            variablesJson=""
        )

    def shutdown(self) -> None:
        """Shutdown the logging system."""
        self._is_shutdown = True
        # Strategic print: logging is being shut down.
        print("CRITICAL LOGGING NOTICE: Logger has been shut down. Subsequent log attempts will print directly to the console.")

    def log(self, level: AllowedLogLevels, message: str, variablesJson: str = "") -> None:
        """
        Log a message with the given level. If logging has been shut down, print a clear message.
        """
        if self._is_shutdown:
            print(f"\n*** CRITICAL LOGGING ERROR: Attempted to log after shutdown. Message: {message}, Variables: {variablesJson} ***\n")
            return

        if self.storageFunction is None:
            self.set_storage_function()

        loggingTableModel = LoggingTableModel(
            level=level, message=message, variablesJson=variablesJson
        )
        loggingTable = LoggingTable.fromModel(loggingTableModel)
        if self.storageFunction is not None:
            try:
                self.storageFunction(logTable=loggingTable)
            except Exception as log_err:
                # Strategic error print: if the log storage fails, show the error details.
                print(f"\n*** CRITICAL ERROR: Failed to log to disk. Error: {log_err}. Message attempted: {message}, Variables: {variablesJson} ***\n")
        else:
            # Should not occur because set_storage_function should have set it.
            print(f"\n*** CRITICAL ERROR: Log storage function not set. Unable to log message: {message}, Variables: {variablesJson} ***\n")
