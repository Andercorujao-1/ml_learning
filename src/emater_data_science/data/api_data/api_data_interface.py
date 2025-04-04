import threading
import time
import json
import polars as pl
import asyncio

# Adjust the import paths according to your project structure.
from emater_data_science.data.api_data.generic_api_fetcher import GenericApiFetcher
class ApiDataInterface:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ApiDataInterface, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True

    def fGetTablesList(self )-> list[str]:
        return []

    def fStoreTable(self,model, data) -> None:
        print(f"Simulating API POST /store. Data: {json.dumps([d.__dict__ for d in data], indent=2)}")

    def fFetchTable(self, tableName, callback, tableFilter) -> None:
        threading.Thread(target=lambda: (time.sleep(1), callback(pl.DataFrame())), daemon=True).start()

    def fDeleteRows(self, tableName, tableFilter) -> None:
        print(f"Simulating API DELETE /delete for table {tableName} with filter: {json.dumps(tableFilter, indent=2)}")

    def fShutdown(self) -> None:
        print("Simulating API connection shutdown.")

    def fApiRequest(self, config) -> pl.DataFrame:
        # Run the asynchronous fFetch of GenericApiFetcher synchronously.
        fetcher = GenericApiFetcher()
        df = asyncio.run(fetcher.fFetch(config))
        return df

