import aiohttp
import polars as pl
import asyncio
from emater_data_science.data.api_data.api_request_model import ApiRequestModel


class GenericApiFetcher:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GenericApiFetcher, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True

    async def fFetch(self, config: ApiRequestModel) -> pl.DataFrame:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    config.method, f"{config.url}/{config.endpoint}", params=config.parameters
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
        except Exception as e:
            # Try to extract the HTTP status code, default to 500 if not available.
            errorCode = getattr(e, "status", 500)
            if errorCode in config.errorMapping:
                config.errorMapping[errorCode](config)
            else:
                from emater_data_science.logging.log_in_disk import LogInDisk
                LogInDisk().log(
                    level="ERROR",
                    message="GenericApiFetcher::fFetch - No handler found for error.",
                    variablesJson=f"errorCode={errorCode}, endpoint={config.endpoint}, error={e}"
                )
            return pl.DataFrame()
        return pl.DataFrame(data)

# Minimal test example.
if __name__ == "__main__":
    async def main():
        # Create a sample API request configuration.
        config = ApiRequestModel(
            url="https://api.example.com",
            endpoint="weather",
            method="GET",
            parameters={"lat": "0", "lon": "0"},
            errorMapping={
                413: lambda cfg: print(f"Handling error 413 for {cfg.endpoint} (retry {cfg.retryCount})")
            }
        )
        fetcher = GenericApiFetcher()
        df = await fetcher.fFetch(config)
        print(df)

    asyncio.run(main())
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fShutdown()
