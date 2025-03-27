#model_api_metadata.py
import logging
from pydantic import BaseModel
from typing import Any, List, Dict


class EndpointMetadataModel(BaseModel):
    name: str
    endpointURL: str
    update_interval_in_days: int
    table_classes: List[Any]
    params: Dict

    @staticmethod
    def fromJson(endpointDataDict):
        try:
            return EndpointMetadataModel(**endpointDataDict)
        except Exception as e:
            logging.error(f"Unexpected error for dataclass EndpointMetadata: {endpointDataDict} {e}")
            raise


class APIMetadataModel(BaseModel):
    name: str
    url: str
    endpoints: List[EndpointMetadataModel]
    otherData: Dict = {}

    @staticmethod
    def fromJson(apiDataDict):
        try:
            return APIMetadataModel(**apiDataDict)
        except Exception as e:
            logging.error(f"Unexpected error for dataclass APIMetadata: {apiDataDict['name']} {e}")
            raise
