import logging
import asyncio
import aiohttp
import importlib

class ApiRequest:
    """ApiRequests gets one ApiConfig object and creates an object
    """
    def __init__(self, api_config, storage):
        self.url: str = api_config.url
        self.endpoints = api_config.endpoints
        self.storage = storage

    def fetch_all(self):
        #request all api endpoints and save them on the storage.save_data()
        loop = asyncio.get_event_loop()
        tasks = []
        for endpoint in self.endpoints:
            params_function = self._get_callable(endpoint.param_list_function)
            param_list = params_function()
            table_schema_classes = []
            for table_schema_string in endpoint.data_class:
                table_schema_classes.append(self._get_class(table_schema_string)) 
            for params in param_list:
                tasks.append(self._async_fetch_and_save(endpoint.endpoint, params, table_schema_classes))
        loop.run_until_complete(asyncio.gather(*tasks))

    async def _async_fetch_and_save(self, endpoint: str, params: dict, table_schema_classes):
        data = await self.fetch(endpoint, params)
        if data:
            self.storage.save_data(table_schema_classes, data)

    async def fetch(self, endpoint: str, params: dict):
        url = self.url + endpoint
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    data = await response.json()
                    logging.info(f"Successfully fetched data from {url}")
                    return data
        except aiohttp.ClientError as e:
            logging.error(f"Error accessing API {url}: {e}")
        except asyncio.TimeoutError:
            logging.error(f"Timeout when accessing API {url}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        return None

    def _get_callable(self, full_name: str):
        module_name, func_name = full_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    def _get_class(self, full_name: str):
        module_name, class_name = full_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)