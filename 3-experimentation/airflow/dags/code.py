from datetime import datetime

from airflow.sdk import dag, task, Param


@dag(
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["monthly", "example"],
    default_args={"owner": "airflow", "retries": 1},
    max_active_runs=1,
    params={
        "year": Param(2023, type="integer", minimum=2009, maximum=2025),
        "month": Param(3, type="integer", minimum=1, maximum=12),
    },
)
def run():
    @task(multiple_outputs=True)
    def get_year_month(**context):
        return {
            "year": context["params"]["year"],
            "month": context["params"]["month"],
        }

    @task(multiple_outputs=True, trigger_rule="all_done")
    def read_dataframe(year, month, suffix, increment=False):
        import pickle
        import pandas as pd

        if increment:
            if month < 12:
                month += 1
            else:
                month = 1
                year += 1

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        df = pd.read_parquet(url)
        original_shape = df.shape
        print(f"Number of records for {month}-{year}: {original_shape[0]}")

        df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
        df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)
        df = df[(df.duration >= 1) & (df.duration <= 60)]

        categorical = ["PULocationID", "DOLocationID"]
        df[categorical] = df[categorical].astype(str)

        processed_shape = df.shape
        print(
            f"Number of preprocessed records for {month}-{year}: {processed_shape[0]}"
        )

        with open(f"/tmp/df_{suffix}.pkl", "wb") as f:
            pickle.dump(df, f)

        return {"original_shape": original_shape, "processed_shape": processed_shape}

    @task(trigger_rule="all_done")
    def create_X(suffix: str):
        import pickle
        from sklearn.feature_extraction import DictVectorizer

        with open(f"/tmp/df_{suffix}.pkl", "rb") as f:
            df = pickle.load(f)

        # categorical = ["PU_DO"]
        categorical = ["PULocationID", "DOLocationID"]
        dicts = df[categorical].to_dict(orient="records")

        if suffix == "train":
            dv = DictVectorizer()
            X = dv.fit_transform(dicts)
        elif suffix == "val":
            with open("/tmp/dv_train.pkl", "rb") as f:
                dv = pickle.load(f)
            X = dv.transform(dicts)
        y = df["duration"].values

        with open(f"/tmp/X_{suffix}.pkl", "wb") as f:
            pickle.dump(X, f)
        with open(f"/tmp/y_{suffix}.pkl", "wb") as f:
            pickle.dump(y, f)
        with open(f"/tmp/dv_{suffix}.pkl", "wb") as f:
            pickle.dump(dv, f)

        return f"/tmp/{suffix}"

    @task
    def train_model():
        import pickle

        import mlflow
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import root_mean_squared_error

        with open("/tmp/X_train.pkl", "rb") as f:
            X_train = pickle.load(f)
        with open("/tmp/y_train.pkl", "rb") as f:
            y_train = pickle.load(f)
        with open("/tmp/X_val.pkl", "rb") as f:
            X_val = pickle.load(f)
        with open("/tmp/y_val.pkl", "rb") as f:
            y_val = pickle.load(f)
        with open("/tmp/dv_train.pkl", "rb") as f:
            dv = pickle.load(f)

        mlflow.set_tracking_uri("http://mlops-zoomcamp-mlflow-1:5000")
        mlflow.set_experiment("nyc-taxi-experiment")
        with mlflow.start_run() as run:
            linear_reg = LinearRegression()
            linear_reg.fit(X_train, y_train)

            y_pred = linear_reg.predict(X_val)
            rmse = root_mean_squared_error(y_val, y_pred)
            mlflow.log_metric("rmse", rmse)

            print(f"RMSE: {rmse:.2f}")
            print(f"Model intercept: {linear_reg.intercept_}")

            with open("dict_vectorizer.bin", "wb") as f:
                pickle.dump(dv, f)
            mlflow.log_artifact("dict_vectorizer.bin")

            mlflow.sklearn.log_model(linear_reg, artifact_path="models_mlflow")

            return run.info.run_id

    @task
    def write_run_id(mlflow_run_id: str):
        with open("/tmp/run_id.txt", "w") as f:
            f.write(mlflow_run_id)
        return mlflow_run_id

    year_month = get_year_month()
    year, month = year_month["year"], year_month["month"]

    # Make the preprocess tasks sequential for now, because of out of memory
    preprocess_task = (
        read_dataframe(year, month, suffix="train")
        >> read_dataframe(year, month, suffix="val", increment=True)
        >> create_X(suffix="train")
        >> create_X(suffix="val")
    )

    model_task = train_model()
    preprocess_task >> model_task
    write_run_id(model_task)


run()
