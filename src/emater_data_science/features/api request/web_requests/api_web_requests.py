import asyncio
import logging
import aiohttp
from emater_data_science_python_api.src.APIs.model_api_metadata import APIMetadataModel, EndpointMetadataModel
from emater_data_science_python_api.src.requests_preprocessing.filter_by_frequency import fFilterByFrequency
from emater_data_science_python_api.src.storage.local_storage import AbstractStorage, LocalStorage


class ApiWebRequests:
    storage: AbstractStorage = LocalStorage()

    async def fRequest(self, apiURL, endpointMetadata:EndpointMetadataModel):
        url = f"{apiURL}/{endpointMetadata.endpointURL}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=endpointMetadata.params, timeout=aiohttp.ClientTimeout(total=10)) as response:
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

    def fRequestAll(self, allApisMetadata:dict[str, APIMetadataModel] = fFilterByFrequency()):
        loop = asyncio.get_event_loop()
        tasks = []
        for (apiName, apiMetadata) in allApisMetadata.items():
            for apiEndpoint in apiMetadata.endpoints:
                tasks.append(self._asyncRequestAndSave(apiMetadata.url, apiEndpoint))
        loop.run_until_complete(asyncio.gather(*tasks))
        

    def fJsonToTableClass(self, data, endpointMetadata:EndpointMetadataModel):
        #TODO check if this can be done without importing table module
        tableClass = endpointMetadata.table_classes[0]
        return tableClass.fromJson(data)
        
    
    async def _asyncRequestAndSave(self, apiURL, endpointMetadata):
        requestResponse = await self.fRequest(apiURL, endpointMetadata)
        print(f"requested {apiURL}")
        dataAsTableObjects = self.fJsonToTableClass(requestResponse, endpointMetadata)
        print(f"turned into objects{apiURL}")
        if dataAsTableObjects:
            self.storage.fSaveData(dataAsTableObjects)
            print(f"stored {apiURL}")

