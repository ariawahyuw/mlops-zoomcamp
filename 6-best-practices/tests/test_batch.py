import batch
import pandas as pd
from datetime import datetime
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():
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
    categorical = [
        "PULocationID",
        "DOLocationID",
    ]
    df = pd.DataFrame(data, columns=columns)
    result_df = batch.prepare_data(df, categorical=categorical)
    result_df = result_df.to_dict()

    expected_data = [
        ("-1", "-1", dt(1, 1), dt(1, 10), 9.0),
        ("1", "1", dt(1, 2), dt(1, 10), 8.0),
    ]

    expected_df = pd.DataFrame(expected_data, columns=columns + ["duration"]).to_dict()

    diff = DeepDiff(result_df, expected_df)

    print("\n\ndiff")
    print(diff)

    assert "type_changes" not in diff
    assert "values_changed" not in diff
