#build_api_metadata_IBGE.py
from emater_data_science_python_api.src.APIs.abstract_build_api_metadata import ApiMetadataBuilder

class ApiMetadataBuilderIBGE(ApiMetadataBuilder):
    packagePath = 'emater_data_science_python_api.src.APIs'
    apiName = 'IBGE'


print(ApiMetadataBuilderIBGE.fBuildApiMetadata())

