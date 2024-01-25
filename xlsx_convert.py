import os
import pandas as pd
from google.cloud import storage


def convert_xlsx_to_csv(
        bucket_name, 
        prefix,
        out_folder
    ):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    
     # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        blob_path = blob.name
        filename = ".".join(blob_path.split("/")[-1].split(".")[:-1]) + ".csv"
        out_file_path = os.path.join(out_folder, filename)
        print(blob_path, filename, out_file_path)

        bucket = storage_client.bucket(bucket_name)
        out_blob = bucket.blob(out_file_path)

        with blob.open("rb") as fin, out_blob.open("wb") as fout:
            df = pd.read_excel(fin)
            df.to_csv(fout, index=False)

if __name__=="__main__":
    convert_xlsx_to_csv(
        "telmark-gcs-development", 
        "mis/productivity/desk collection - fv/Shopee_xlsx/reportcallSHOPEE",
        "mis/productivity/desk collection - fv/Shopee_csv_autoconvert/"
    )