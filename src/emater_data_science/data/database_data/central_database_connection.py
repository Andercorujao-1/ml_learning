from collections.abc import Callable
from datetime import date
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
        self._is_shutting_down = False  # Flag to prevent enqueuing new tasks after shutdown begins.
        self._initialized = True
        self._ensureWorker()

    def _initializeDatabaseEngine(self, db_name: str = "Local_Database.db") -> None:
        dbPath = os.path.join("C:\\emater_data_science", db_name)
        dbUrl = f"sqlite:///{dbPath}"
        self._engine = create_engine(url=dbUrl, echo=False)

    def _enqueue_operation(self, operation: Callable[[], None]) -> None:
        if self._is_shutting_down:
            # Log a warning if needed; here we simply print a warning.
            print("Warning: Attempt to enqueue an operation after shutdown has begun.")
            return
        self._operation_queue.put(operation)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                operation = self._operation_queue.get(timeout=1)
            except Empty:
                continue
            try:
                operation()
            except Exception as op_err:
                # Log the error and continue processing remaining operations.
                print(f"Error during operation: {op_err}")
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
        dateColumn: str | None = None,
        startDate: date | None = None,
        endDate: date | None = None
    ) -> None:
        from emater_data_science.logging.log_in_disk import LogInDisk

        LogInDisk().log(
            level="executionState",
            message="CentralDatabaseConnection::fRead - Called.",
            variablesJson=f"tableName={tableName}",
        )

        def operation() -> None:
            LogInDisk().log(
                level="executionState",
                message="CentralDatabaseConnection::fRead - Operation started execution.",
                variablesJson=f"tableName={tableName}",
            )
            if self._engine is None:
                raise ValueError("Database engine not initialized.")

            self.metadata.reflect(bind=self._engine, only=[tableName])
            if tableName not in self.metadata.tables:
                raise ValueError(f"Table '{tableName}' not found.")

            table = self.metadata.tables[tableName]
            query = select(table)

            # Date filtering
            if dateColumn and (startDate or endDate):
                if dateColumn not in table.c:
                    raise ValueError(f"Date column '{dateColumn}' not found in table '{tableName}'")
                if startDate:
                    query = query.where(table.c[dateColumn] >= startDate)
                if endDate:
                    query = query.where(table.c[dateColumn] <= endDate)

            # General column = value filters
            if tableFilter:
                for col, val in tableFilter.items():
                    if col in table.c:
                        query = query.where(table.c[col] == val)

            with self._engine.connect() as conn:
                results = conn.execute(statement=query).fetchall()

            results_dicts = [dict(row._mapping) for row in results]

            try:
                df = pl.DataFrame(results_dicts, infer_schema_length=None)
            except Exception as err:
                from pprint import pprint
                print("\n--- Failed to create DataFrame ---")
                print(f"Error: {err}")
                print(f"Sample rows ({min(5, len(results_dicts))}):")
                pprint(results_dicts[:5])
                raise

            if callback:
                callback(df)

        self._enqueue_operation(operation)

    def fQueueIsEmpty(self) -> bool:
        return self._operation_queue.empty()
    

    def fWrite(self, model: type[T], data: pl.DataFrame) -> None:
       
        table_obj: Table = cast(Table, model.__table__)
        sample = data.head(2).to_dicts()
        from emater_data_science.logging.logging_table_model import LoggingTable
        from emater_data_science.logging.log_in_disk import LogInDisk

        # Log only if this operation was enqueued before shutdown began.
        if not self._stop_event.is_set() and not issubclass(model, LoggingTable):
            LogInDisk().log(
                level="executionState",
                message="CentralDatabaseConnection::fWrite - Called.",
                variablesJson=f"data (truncated)={sample}",
            )

        if self._engine is None:
            raise ValueError("Database engine not initialized.")
        engine = self._engine
        if not inspect(engine).has_table(table_obj.name):
            def createTable() -> None:
                if inspect(engine).has_table(table_obj.name):
                    return
                print(f'creating table {table_obj.name}')
                LogInDisk().log(
                    level="executionState",
                    message=f"CentralDatabaseConnection::fWrite - Table '{table_obj.name}' not found. Creating table.",
                    variablesJson="",
                )
                try:
                    table_obj.create(bind=engine)
                    self.metadata.reflect(bind=engine, only=[table_obj.name])
                except Exception:
                    print(
                        f"\n*** CRITICAL ERROR: Exception while creating table '{table_obj.name}'. Data: {sample} ***\n"
                    )
                    raise

            self._enqueue_operation(createTable)

        def insertData() -> None:
            print(f"inserting data {table_obj.name}")
            nonlocal data
            if not self._stop_event.is_set() and not issubclass(model, LoggingTable):
                LogInDisk().log(
                    level="executionState",
                    message="CentralDatabaseConnection::fWrite - Operation started execution.",
                    variablesJson=f"data (truncated)={sample}",
                )
            dataAsDict = data.to_dicts()
            del data
            import gc
            gc.collect()

            try:
                with engine.connect() as conn:
                    stmt = insert(table_obj)
                    conn.execute(stmt, dataAsDict)
                    conn.commit()
            except Exception:
                print(
                    f"\n*** CRITICAL ERROR: Exception during bulk insert into '{table_obj.name}'. Data: {sample} ***\n"
                )
                raise

        self._enqueue_operation(insertData)

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

        self._enqueue_operation(operation)

    def fListTables(self) -> list[str]:
        if self._engine is None:
            raise ValueError("Database engine not initialized.")
        self.metadata.reflect(bind=self._engine)
        tables = list(self.metadata.tables.keys())
        return tables

    def fShutdown(self) -> None:
        # Mark shutdown so that no new operations will be enqueued.
        self._is_shutting_down = True
        self._operation_queue.join()
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join()
        if self._engine:
            self._engine.dispose()
