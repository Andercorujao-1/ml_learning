#api_loading.py
import json
import logging
import importlib.resources


from .model_api_metadata import APIMetadataModel


def fLoadAPIMetadata(packagePath: str, apiName: str) -> APIMetadataModel:
    packagePath = f"{packagePath}.{apiName}"
    apiInterfaceFilename = f"interface_{apiName}"
    try:
        module = importlib.import_module(f"{packagePath}.{apiInterfaceFilename}")
        apiMetadata = module.fGetMetadata()
        logging.info(f"Loaded configuration for API: {apiInterfaceFilename}")
        return apiMetadata
    except Exception as e:
        logging.error(f"Unexpected error fLoadAPIMetadata for {apiName}: {e}")
        raise


def fLoadAllAPIsMetadata(packagePath="emater_data_science_python_api.src.APIs") -> dict[str,APIMetadataModel]:
    filename = "apiList.json"
    try:
        with importlib.resources.open_text(packagePath, filename) as f:
            apiNamesList = json.load(f).get("apis", [])
    except Exception as e:
        logging.error(f"Unexpected error loading fLoadAllAPIsMetadata on file loading: {e}")
        raise

    apiMetadataDict = {}
    for apiName in apiNamesList:
        apiMetadata = fLoadAPIMetadata(packagePath, apiName)
        apiMetadataDict[apiName] = apiMetadata
    return apiMetadataDict
