import os
from typing import Dict

from dagster import asset

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .yahoo_utils import scrape_stock_measures

from ..blob_utils import (
    download_parquet_blob_to_df,
    upload_to_blob,
)


@asset(group_name='new_stock_data', compute_kind='pandas')
def get_snp500_from_yahoo() -> pd.DataFrame:
    stocks = pd.read_csv("/workspaces/codespaces-jupyter/data/constituents_csv.csv")
    return scrape_stock_measures(stocks=stocks)["stock_measures"]


@asset(compute_kind='Dict', required_resource_keys={'blob_service_client'})
def append_new_snp500_data_to_old(context, get_snp500_from_yahoo) -> pd.DataFrame:

    old_snp500_df = download_parquet_blob_to_df(
        blob_service_client=context.resources.blob_service_client,
        container_name="first-stock-blob",
        blob_name="stock_measures.parquet",
    )
    return pd.concat([old_snp500_df, get_snp500_from_yahoo], axis=0)


@asset(required_resource_keys={'blob_service_client'})
def upload_snp500_data_to_blob(context, append_new_snp500_data_to_old) -> None:

    table = pa.Table.from_pandas(append_new_snp500_data_to_old)
    
    # Create a BytesIO buffer to hold the Parquet data, write the Table to the buffer in Parquet format
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)

    parquet_data =  buffer.getvalue().to_pybytes()
    upload_to_blob(
        blob_service_client=context.resources.blob_service_client,
        container_name="first-stock-blob",
        filename="stock_measures.parquet",
        data=parquet_data
    )
