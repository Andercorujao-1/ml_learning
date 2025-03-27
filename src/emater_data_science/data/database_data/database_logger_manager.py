import threading
from emater_data_science.data.database_data.central_database_connection import CentralDatabaseConnection

class DatabaseLoggerManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DatabaseLoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self.buffer_size = 30
        self.flush_interval = 2.0
        self.buffer = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.flush_thread = None
        self._initialized = True
        self._initialize()

    def _initialize(self) -> None:
        self.stop_event.clear()  # Ensure the stop event is cleared.
        if self.flush_thread is None or not self.flush_thread.is_alive():
            self.flush_thread = threading.Thread(target=self._flush_periodically, daemon=True)
            self.flush_thread.start()

    def fStoreLog(self, log) -> None:
        # If shutdown has been initiated, print a clear warning and do not store the log.
        if self.stop_event.is_set():
            print(
                "DATABASE LOGGER MANAGER WARNING: Attempted to store log after shutdown; log discarded. "
                "log.level=", log.level, "log.message=", log.message, log.variablesJson
            )
            return
        with self.lock:
            self.buffer.append(log)
            if len(self.buffer) >= self.buffer_size:
                self._flush()

    def _flush(self) -> None:
        with self.lock:
            if not self.buffer:
                return
            logs_to_write = self.buffer.copy()
            self.buffer.clear()
        # Call fWrite directly so that, in case of an error, fWrite prints full data and raises.
        CentralDatabaseConnection().fWrite(data=logs_to_write)

    def _flush_periodically(self) -> None:
        while not self.stop_event.is_set():
            if self.stop_event.wait(timeout=self.flush_interval):
                break
            # If _flush raises an exception, let it propagate.
            self._flush()
        # Final flush after stop event is set.
        self._flush()

    def fShutdown(self) -> None:
        self.stop_event.set()
        if self.flush_thread:
            self.flush_thread.join()
            self.flush_thread = None

if __name__ == "__main__":
    from emater_data_science.logging.log_in_disk import LogInDisk
    LogInDisk().log(level="userAction", message="variablesJson=Teste")
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fShutdown()
   