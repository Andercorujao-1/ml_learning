import polars as pl
from datetime import datetime

from emater_data_science_python_api.src.APIs.interface_apis import fLoadAllAPIsMetadata, APIMetadataModel
from emater_data_science_python_api.src.storage.abstract_storage import AbstractStorage
from emater_data_science_python_api.src.storage.local_storage import LocalStorage


def fGetMostRecentRequestDate(requestsHistory: pl.DataFrame, apiName: str, endpointName: str) -> datetime:
    try:
        filtered = requestsHistory.filter(
            (requestsHistory["apiName"] == apiName) & (requestsHistory["endpointName"] == endpointName)
        )
        if filtered.is_empty():
            return datetime.min
        mostRecentDate = filtered.select(pl.col("dateTime").max()).to_series()[0]
        if isinstance(mostRecentDate, str):
            mostRecentDate = datetime.fromisoformat(mostRecentDate)
        else:
            print(f"error, mostRecentdDate from database is of type {type(mostRecentDate)}")
        return mostRecentDate

    except Exception as e:
        print(f"Error in fGetMostRecentRequestDate for {apiName} - {endpointName}: {e}")
        raise


def fFilterByFrequency(
    storage: AbstractStorage = LocalStorage(),
    apisDict: dict[str, APIMetadataModel] = fLoadAllAPIsMetadata(),
) -> dict[str, APIMetadataModel]:
    requestsHistory = storage.fGetTableContents("requests_history")
    if requestsHistory is None:
        return apisDict

    filteredMetadata = {}
    currentDay = datetime.now()
    for apiName, apiMetadata in apisDict.items():
        filteredEndpoints = []
        
        for endpoint in apiMetadata.endpoints:
            daysSinceLastRequest = (
                currentDay - fGetMostRecentRequestDate(requestsHistory, apiName, endpoint.name)
            ).days
            if daysSinceLastRequest >= endpoint.update_interval_in_days:
                filteredEndpoints.append(endpoint)

        if filteredEndpoints:
            filteredMetadata[apiName] = apiMetadata
            filteredMetadata[apiName].endpoints = filteredEndpoints

    return filteredMetadata
