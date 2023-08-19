import os

from dagster import asset

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils import scrape_stock_measures

from blob_utils import (
    get_blob_service_client_connection_string,
    download_parquet_blob_to_df,
    upload_to_blob,
)


@asset
def get_snp500_from_yahoo():
    stocks = pd.read_csv("/workspaces/codespaces-jupyter/data/constituents_csv.csv")
    stock_measure_df = scrape_stock_measures(stocks=stocks)["stock_measures"]

    table = pa.Table.from_pandas(stock_measure_df)
    
    # Create a BytesIO buffer to hold the Parquet data, write the Table to the buffer in Parquet format
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)

    return buffer.getvalue().to_pybytes

@asset
def append_new_snp500_data_to_old(get_snp500_from_yahoo) -> pd.DataFrame:

    azure_storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = get_blob_service_client_connection_string(
        azure_storage_connection_string=azure_storage_connection_string
    )

    old_snp500_df = download_parquet_blob_to_df(
        blob_service_client=blob_service_client,
        container_name="first-stock-blob",
        blob_name="stock_measures.parquet",
    )

    return pd.concat([old_snp500_df, get_snp500_from_yahoo], axis=0)



@asset
def upload_snp500_data_to_blob(append_new_snp500_data_to_old):
    upload_to_blob(
        blob_service_client=append_new_snp500_data_to_old.blob_service_client,
        container_name="first-stock-blob",
        blob_name="stock_measures.parquet",
    )
