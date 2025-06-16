#!/usr/bin/env python
# coding: utf-8

import sys

import pickle
import pandas as pd


categorical = ["PULocationID", "DOLocationID"]


def read_data(filename):
    df = pd.read_parquet(filename)

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def load_model(filename: str):
    with open(filename, "rb") as f_in:
        dv, model = pickle.load(f_in)
    return dv, model


def predict(year, month):
    df = read_data(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )

    dv, model = load_model("model.bin")

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)
    print("y_pred mean:", y_pred.mean())
    print("y_pred standard deviation:", y_pred.std())

    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")
    df["predictions"] = y_pred
    df_result = df[["ride_id", "predictions"]].copy()
    output_file = "duration_predictions.parquet"
    df_result.to_parquet(output_file, engine="pyarrow", compression=None, index=False)


def main():
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    predict(year, month)


if __name__ == "__main__":
    main()
