#!/usr/bin/env python
# coding: utf-8

import os
import subprocess
from datetime import datetime

import boto3
import botocore.exceptions
import pandas as pd
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def s3_file_exists(s3_client, s3_uri):
    path_parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket = path_parts[0]
    key = path_parts[1]
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        print(f"Filesize of {key} is:", int(response["ContentLength"]))
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def test_save_data_s3(year, month):
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
    s3_client = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)

    input_pattern = os.getenv("INPUT_FILE_PATTERN")
    input_file = input_pattern.format(year=year, month=month)

    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]
    columns = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]
    df = pd.DataFrame(data, columns=columns)
    if S3_ENDPOINT_URL:
        options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
        df.to_parquet(
            input_file,
            engine="pyarrow",
            compression=None,
            index=False,
            storage_options=options,
        )
    else:
        df.to_parquet(
            input_file,
            engine="pyarrow",
            compression=None,
            index=False,
        )

    assert s3_file_exists(s3_client, input_file), (
        f"Input file does not exist in S3 URI: {input_file}"
    )


def test_output_data(year, month):
    parent_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.path.pardir)
    )
    os.chdir(parent_dir)
    subprocess.run(["python", "batch.py", str(year), str(month)])

    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
    if S3_ENDPOINT_URL:
        options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
        df = pd.read_parquet(
            os.getenv("OUTPUT_FILE_PATTERN").format(year=year, month=month),
            storage_options=options,
        )
    else:
        df = pd.read_parquet(
            os.getenv("OUTPUT_FILE_PATTERN").format(year=year, month=month),
        )

    result_data = df.to_dict(orient="records")
    expected_data = [
        {"ride_id": f"{year:04d}/{month:02d}_0", "predicted_duration": 23.1971},
        {"ride_id": f"{year:04d}/{month:02d}_1", "predicted_duration": 13.0801},
    ]

    diff = DeepDiff(result_data, expected_data, significant_digits=4)

    print("\n\ndiff")
    print(diff)

    assert "type_changes" not in diff
    assert "values_changed" not in diff

    print(
        f"The sum of predicted durations for the test dataframe is: {df['predicted_duration'].sum().round(2)}"
    )


if __name__ == "__main__":
    test_save_data_s3(2023, 1)
    test_output_data(2023, 1)
