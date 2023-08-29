"""
Utility functions for interacting with azure blob storage.
"""

import os
from io import BytesIO
import pandas as pd
from azure.storage.blob import BlobServiceClient


def get_blob_service_client_connection_string(
    azure_storage_connection_string: str,
) -> BlobServiceClient:
    """
    Get a BlobServiceClient instance using the provided Azure Storage connection string and account name.

    Args:
        azure_storage_connection_string (str): The Azure Storage connection string.
        azure_storage_account_name (str): The Azure Storage account name.

    Returns:
        BlobServiceClient: A BlobServiceClient instance.
    """
    
    connection_string = azure_storage_connection_string

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    return blob_service_client


def upload_to_blob(
    blob_service_client: BlobServiceClient,
    container_name: str,
    filename: str,
    data
) -> None:
    """
    Upload a file as a blob to a specified Azure Storage container.

    Args:
        blob_service_client (BlobServiceClient): A BlobServiceClient instance.
        container_name (str): The name of the Azure Storage container.
        filename (str): The name to give to the blob.
        date: data to upload.

    Returns:
        None
    """
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    
    container_client.upload_blob(name=filename, data=data, overwrite=True)


def upload_to_blob_from_file(
    blob_service_client: BlobServiceClient,
    container_name: str,
    filepath: str,
    filename: str,
) -> None:
    """
    Upload a file as a blob to a specified Azure Storage container.

    Args:
        blob_service_client (BlobServiceClient): A BlobServiceClient instance.
        container_name (str): The name of the Azure Storage container.
        filepath (str): The local path to the file to be uploaded.
        filename (str): The name to give to the blob.

    Returns:
        None
    """
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    with open(file=os.path.join(filepath, filename), mode="rb") as data:
        container_client.upload_blob(name=filename, data=data, overwrite=True)


def download_parquet_blob_to_df(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
) -> pd.DataFrame:
    """
    Download a Parquet blob from a specified Azure Storage container and load it into a Pandas DataFrame.

    Args:
        blob_service_client (BlobServiceClient): A BlobServiceClient instance.
        container_name (str): The name of the Azure Storage container.
        blob_name (str): The name of the blob to download.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the data from the Parquet blob.
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    download_stream = blob_client.download_blob()
    stream = BytesIO(download_stream.readall())

    return pd.read_parquet(stream)
