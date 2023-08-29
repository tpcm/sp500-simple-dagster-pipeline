import os

from dagster import StringSource, resource

from .blob_utils import get_blob_service_client_connection_string


@resource(config_schema={'azure_storage_connection_string':StringSource})
def get_blob_service_client(init_context):
    return get_blob_service_client_connection_string(
        azure_storage_connection_string=init_context.resource_config['azure_storage_connection_string']
    )

# def get_blob_service_client(init_context):
#     azure_storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
#     return get_blob_service_client_connection_string(
#         azure_storage_connection_string=azure_storage_connection_string
#     )da