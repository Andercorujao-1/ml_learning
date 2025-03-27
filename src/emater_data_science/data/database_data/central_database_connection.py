from collections.abc import Callable
import os
from threading import Thread, Lock, Event
from queue import Queue, Empty
from typing import TypeVar, cast
import polars as pl
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (
    Engine,
    Table,
    create_engine,
    MetaData,
    select,
    insert,
    delete,
    inspect,
)

T = TypeVar("T", bound=DeclarativeBase)


class CentralDatabaseConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CentralDatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._operation_queue: Queue = Queue()
        self._worker_thread: Thread | None = None
        self._stop_event: Event = Event()
        self._lock = Lock()
        self._engine: Engine | None = None
        self.metadata = MetaData()
        self._initialized = True
        self._ensureWorker()

    def _initializeDatabaseEngine(self, db_name: str = "Local_Database.db") -> None:
        userprofile = os.environ.get("USERPROFILE")
        if not userprofile:
            raise EnvironmentError("Environment variable 'USERPROFILE' is not set.")
        dbPath = os.path.join(
            userprofile,
            "OneDrive",
            "Documentos",
            "projects_data",
            "emater_data_science",
            db_name,
        )
        dbUrl = f"sqlite:///{dbPath}"
        self._engine = create_engine(url=dbUrl, echo=False)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                operation = self._operation_queue.get(timeout=1)
            except Empty:
                continue
            try:
                operation()
            except Exception as op_err:
                raise Exception("CentralDatabaseConnection::_worker - Error during operation.") from op_err
            finally:
                self._operation_queue.task_done()

    def _ensureWorker(self) -> None:
        with self._lock:
            if self._engine is None:
                self._initializeDatabaseEngine()
            if self._worker_thread is None or not self._worker_thread.is_alive():
                self._stop_event.clear()
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()

    def fRead(
        self,
        tableName: str,
        callback: Callable[[pl.DataFrame], None] | None,
        tableFilter: dict | None = None,
    ) -> None:
        from emater_data_science.logging.log_in_disk import LogInDisk

        LogInDisk().log(
            level="executionState",
            message="CentralDatabaseConnection::fRead - Called.",
            variablesJson=f"tableName={tableName}",
        )

        def operation() -> None:
            from emater_data_science.logging.log_in_disk import LogInDisk

            LogInDisk().log(
                level="executionState",
                message="CentralDatabaseConnection::fRead - Operation started execution.",
                variablesJson=f"tableName={tableName}",
            )
            if self._engine is None:
                raise ValueError("Database engine not initialized.")
            self.metadata.reflect(bind=self._engine, only=[tableName])
            if tableName not in self.metadata.tables:
                error_message = f"Table '{tableName}' not found."
                print(f"\n*** CRITICAL ERROR: {error_message} ***\n")
                raise ValueError(error_message)
            table = self.metadata.tables[tableName]
            query = select(table)
            if tableFilter:
                for col, val in tableFilter.items():
                    if col in table.c:
                        query = query.where(table.c[col] == val)
            with self._engine.connect() as conn:
                results = conn.execute(statement=query).fetchall()
            results_dicts = [dict(row._mapping) for row in results]
            df = pl.DataFrame(results_dicts)
            if callback:
                callback(df)

        self._operation_queue.put(operation)

    def fWrite(self, data: list[T]) -> None:
        print('central database conections fWrite')
        from emater_data_science.logging.log_in_disk import LogInDisk

        truncated_data = data[:3]
        LogInDisk().log(
            level="executionState",
            message="CentralDatabaseConnection::fWrite - Called.",
            variablesJson=f"data (truncated)={truncated_data}",
        )

        def operation() -> None:
            from emater_data_science.logging.log_in_disk import LogInDisk

            LogInDisk().log(
                level="executionState",
                message="CentralDatabaseConnection::fWrite - Operation started execution.",
                variablesJson=f"data (truncated)={truncated_data}",
            )
            if not data:
                return
            if self._engine is None:
                raise ValueError("Database engine not initialized.")
            engine = self._engine
            table_obj: Table = cast(Table, type(data[0]).__table__)
            if not inspect(engine).has_table(table_obj.name):
                from emater_data_science.logging.log_in_disk import LogInDisk

                LogInDisk().log(
                    level="executionState",
                    message=f"CentralDatabaseConnection::fWrite - Table '{table_obj.name}' not found. Creating table.",
                    variablesJson="",
                )
                try:
                    table_obj.create(bind=engine)
                    self.metadata.reflect(bind=engine, only=[table_obj.name])
                except Exception:
                    # Print the full data (no truncation) without printing the error details.
                    print(
                        f"\n*** CRITICAL ERROR: Exception while creating table '{table_obj.name}'. Data: {data} ***\n"
                    )
                    raise
            try:
                data_dicts = [
                    {col.name: getattr(obj, col.name) for col in table_obj.columns}
                    for obj in data
                ]
            except Exception:
                print(
                    f"\n*** CRITICAL ERROR: Exception converting objects. Data: {data} ***\n"
                )
                raise
            try:
                with engine.connect() as conn:
                    stmt = insert(table_obj)
                    conn.execute(stmt, data_dicts)
                    conn.commit()
            except Exception:
                print(
                    f"\n*** CRITICAL ERROR: Exception during bulk insert into '{table_obj.name}'. Data: {data} ***\n"
                )
                raise

        self._operation_queue.put(operation)

    def fListTables(self) -> list[str]:
        if self._engine is None:
            raise ValueError("Database engine not initialized.")
        self.metadata.reflect(bind=self._engine)
        tables = list(self.metadata.tables.keys())
        return tables

    def fDeleteRows(self, table: Table, tableFilter: dict) -> None:
        from emater_data_science.logging.log_in_disk import LogInDisk

        LogInDisk().log(
            level="executionState",
            message="CentralDatabaseConnection::fDelete - Called.",
            variablesJson=f"table={table.name}",
        )

        def operation() -> None:
            from emater_data_science.logging.log_in_disk import LogInDisk

            LogInDisk().log(
                level="executionState",
                message="CentralDatabaseConnection::fDelete - Operation started execution.",
                variablesJson=f"table={table.name}",
            )
            if self._engine is None:
                raise ValueError("Database engine not initialized.")
            with self._engine.connect() as conn:
                stmt = delete(table=table)
                for col, val in tableFilter.items():
                    if col in table.c:
                        stmt = stmt.where(table.c[col] == val)
                conn.execute(statement=stmt)
                conn.commit()

        self._operation_queue.put(operation)

    def fShutdown(self) -> None:
        self._operation_queue.join()  # Wait for all operations to complete.
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=200)
        if self._engine:
            self._engine.dispose()
