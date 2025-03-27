#interface_IBGE.py
from emater_data_science_python_api.src.APIs.IBGE.build_api_metadata_IBGE import ApiMetadataBuilderIbge


def fGetMetadata():
    return ApiMetadataBuilderIbge.fBuildApiMetadata()
