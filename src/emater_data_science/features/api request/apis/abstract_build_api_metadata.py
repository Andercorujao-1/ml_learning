#abstract_build_api_metadata.py
import importlib.resources
import json
import logging
from emater_data_science_python_api.src.APIs.model_api_metadata import APIMetadataModel, EndpointMetadataModel


class ApiMetadataBuilder:
    packagePath = ""
    apiName = ""

    @classmethod
    def fGetEndpointList(cls)->list:
        try:
            with importlib.resources.open_text(f"{cls.packagePath}.{cls.apiName}", "endpointList.json") as f:
                endpointList:list = json.load(f).get("endpoints", [])
            return endpointList
        except FileNotFoundError:
            logging.error(
                f"Main .json endpoint list file endpointList.json not found in package {cls.packagePath}.{cls.apiName}"
            )
            raise
        except Exception as e:
            logging.error(f"Unexpected error fGetEndpointList: {e}")
            raise

    @classmethod
    def fGetAPIJsonData(cls)->dict:
        try:
            with importlib.resources.open_text(f"{cls.packagePath}.{cls.apiName}", f"metadata_{cls.apiName}.json") as f:
                apiJsonData = json.load(f)
            return apiJsonData
        except FileNotFoundError:
            logging.error(
                f"Main .json api data file metadata_{cls.apiName} not found in package {cls.packagePath}.{cls.apiName}"
            )
            raise
        except Exception as e:
            logging.error(f"Unexpected error fGetAPIData: {e}")
            raise

    @classmethod
    def fGetEndpointMetadata(cls, endpointName)->EndpointMetadataModel:
        try:
            with importlib.resources.open_text(
                f"{cls.packagePath}.{cls.apiName}", f"{endpointName}_metadata.json"
            ) as f:
                endpointJsonData = json.load(f)
            endpointData = EndpointMetadataModel.fromJson(endpointJsonData)
            endpointData = cls.fLoadTableClasses(endpointData)
            return endpointData
        except FileNotFoundError:
            logging.error(
                f"Main .json endpoint data file {endpointName}.metadata not found in package {cls.packagePath}.{cls.apiName}"
            )
            raise
        except Exception as e:
            logging.error(f"Unexpected error fGetEndpointMetadata: {e}")
            raise

    @classmethod
    def fLoadTableClasses(cls, endpointMetadata: EndpointMetadataModel):
        tableClassesList = []
        try:
            module = importlib.import_module(f"{cls.packagePath}.{cls.apiName}.{endpointMetadata.name}_tables")
            for tableName in endpointMetadata.table_classes:
                if not isinstance(tableName, str):
                    raise TypeError(f"Table name {tableName} is not a string")
                try:
                    tableClass = getattr(module, tableName)
                    tableClassesList.append(tableClass)
                except AttributeError:
                    raise ImportError(f"Table class {tableName} not found in module {module.__name__}")
        except Exception as e:
            logging.error(f"Unexpected error in fLoadTableClasses for endpoint {endpointMetadata.name}: {e}")
            raise

        endpointMetadata.table_classes = tableClassesList
        return endpointMetadata

    @classmethod
    def fBuildApiMetadata(cls)->APIMetadataModel:
        apiJsonData = cls.fGetAPIJsonData()
        endpointNames = cls.fGetEndpointList()
        endpointMetadataList:list[EndpointMetadataModel] = []
        for endpointName in endpointNames:
            endpointData = cls.fGetEndpointMetadata(endpointName)
            endpointMetadataList.append(endpointData)

        apiMetadataDict = {"name": cls.apiName, "endpoints": endpointMetadataList}
        apiMetadataDict.update(**apiJsonData)
        try:
            apiMetadata = APIMetadataModel.fromJson(apiMetadataDict)
            return apiMetadata
        except Exception as e:
            logging.error(f"Unexpected error fBuildApiMetadata: {e}")
            raise