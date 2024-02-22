import os
from typing import List, Any, Optional, Union
from datetime import datetime

import pandas as pd
from dagster import asset
from dagster_gcp import BigQueryResource
from google.cloud import storage
from google.cloud import bigquery as bq


VALID_COLNAMES = [
    "USERID",
    "TL",
    "COLLECTORNAME",
    "NAMEPROJECT",
    "USERID.1",
    "CAMPAIGN",
    "DEBTORNAME",
    "DPD",
    "AMOUNTDUE",
    "STATUS",
    "SUBSTATUS",
    "CALLDURATION",
    "CALLDATE",
    "TIMEDATE",
    "ACTIVITYCODE",
    "PTPAMOUNT",
    "PTPDATE",
    "SPOKEWITH",
    "CONTACTEDNUMBER",
    "REMARKS",
    "LOANTYPE",
    "BUCKETR",
    "CASEID2"
]


def convert_xlsx_to_csv(
        bucket_name, 
        prefix,
        out_folder,
        rejected_folder
    ):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    
     # Note: The call returns a response only when the iterator is consumed.
    converted_files = []
    for blob in blobs:
        blob_path = blob.name
        if not blob_path.endswith(".xlsx"): continue
        filename = ".".join(blob_path.split("/")[-1].split(".")[:-1]) + ".csv"

        # check if file exists in the output folder
        out_file_path = os.path.join(out_folder, filename)
        output_list = storage_client.list_blobs(bucket_name, prefix=out_file_path)
        file_exists = [blob.name for blob in output_list if blob.name==out_file_path]
        if len(file_exists) > 0: continue
        
        # check if file exists in the rejected folder
        rej_file_path = os.path.join(rejected_folder, filename)
        rej_list = storage_client.list_blobs(bucket_name, prefix=rej_file_path)
        rej_exists = [blob.name for blob in rej_list if blob.name==rej_file_path]
        if len(rej_exists) > 0: continue

        # if not exists in both, then proceed with conversion
        bucket = storage_client.bucket(bucket_name)
        print(f"reading: {out_file_path}")
        is_rejected = False
        with blob.open("rb") as fin:
            df = pd.read_excel(fin)
            df.columns = [c.upper() for c in df.columns]
            if 'TL' not in df.columns:
                df['TL'] = None
            try:
                df = df[VALID_COLNAMES]
                df['filename'] = filename
                df['created_at'] = datetime.utcnow()
                output_csv = df.to_csv(index=False)
            except Exception as e:
                out_file_path = os.path.join(rejected_folder, filename)
                output_csv = str(e)
                is_rejected = True

        if not is_rejected:
            converted_files.append(f"gs://{bucket_name}/{out_file_path}")

        out_blob = bucket.blob(out_file_path)
        print(f"writing: {out_file_path}")
        with out_blob.open("w") as fout:
            fout.write(output_csv)
    return converted_files


@asset
def shopee_desk_fv_csv_to_bronze(bigquery: BigQueryResource):
    source_uris = convert_xlsx_to_csv(
        "telmark-gcs-development", 
        "mis/productivity/desk collection - fv/Shopee_xlsx/reportcallSHOPEE",
        "mis/productivity/desk collection - fv/Shopee_csv_autoconvert/",
        "mis/productivity/desk collection - fv/Shopee_csv_rejected/"
    )
    if len(source_uris) == 0:
        return None
    uris = "','".join(source_uris)
    sql = f"""
    LOAD DATA INTO `telmark-gcp.desk_fv_dev_bronze.shopee_call_result_csv`
    (
        userid STRING,
        tl STRING,
        collectorname STRING,
        nameproject STRING,
        userid1 STRING,
        campaign STRING,
        debtorname STRING,
        dpd STRING,
        amountdue STRING,
        status STRING,
        substatus STRING,
        callduration STRING,
        calldate DATE,
        timedate TIME,
        activitycode STRING,
        ptpamount STRING,
        ptpdate STRING,
        spokewith STRING,
        contactednumber STRING,
        remarks STRING,
        loantype STRING,
        bucketr STRING,
        caseid2 STRING,
        filename STRING,
        created_at TIMESTAMP
    )
    FROM FILES (
        format = 'CSV',
        skip_leading_rows=1,
        uris = ['{uris}']
    );
    """
    print(sql)

    with bigquery.get_client() as client:
        job = client.query(sql)
        result_iter = job.result()
    
    print(result_iter)

    return result_iter


@asset(deps=[shopee_desk_fv_csv_to_bronze])
def fact_shopee_call_result(bigquery: BigQueryResource):
    sql = """
    BEGIN TRANSACTION;

    CREATE TEMP TABLE `temp_fact_shopee_call_result`
    AS SELECT
        userid,
        tl,
        collectorname,
        nameproject,
        userid1,
        campaign,
        debtorname,
        dpd,
        amountdue,
        status,
        substatus,
        callduration,
        calldate,
        timedate,
        activitycode,
        ptpamount,
        ptpdate,
        spokewith,
        contactednumber,
        remarks,
        loantype,
        bucketr,
        caseid2,
        filename,
        created_at
    FROM
        `telmark-gcp.desk_fv_dev_bronze.shopee_call_result_csv`
    WHERE
    created_at > (
        SELECT
        max(created_at) max_created_at
        FROM
        `telmark-gcp.desk_fv_dev_silver.fact_shopee_call_result`  
    );

    DELETE FROM
    `telmark-gcp.desk_fv_dev_silver.fact_shopee_call_result`
    WHERE
    filename IN (
        SELECT DISTINCT filename FROM `temp_fact_shopee_call_result`
    );

    INSERT INTO 
    `telmark-gcp.desk_fv_dev_silver.fact_shopee_call_result`
    SELECT * FROM `temp_fact_shopee_call_result`;

    DROP TABLE `temp_fact_shopee_call_result`;

    COMMIT TRANSACTION;
    """

    with bigquery.get_client() as client:
        job = client.query(sql)
        result_iter = job.result()

    print(result_iter)

    return result_iter