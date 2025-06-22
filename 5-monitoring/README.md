# Homework 5

## Question 1
_(in the `baseline_model_nyc_taxi_data.ipynb` jupyter notebook)_

> Answer: number of rows for March 2024 Green Taxi NYC data is 57457.

## Question 2
_(in the `baseline_model_nyc_taxi_data.ipynb` jupyter notebook)_

> Answer: I use the following metrics.
```python
[
    ValueDrift(column="prediction"),
    DriftedColumnsCount(),
    MissingValueCount(column="prediction"),
    QuantileValue(column="fare_amount", quantile=0.5),
    RowCount(),
    EmptyRowsCount(),
]
```

## Question 3
By running:
```bash
> docker compose up
> python evidently_metrics_calculation.py
```
and by looking at Grafana dashboard in: http://localhost:3000 (or looking at adminer at http://localhost:8080), I got: 

__14.2__ as maximum `quantile = 0.5` for fare amount.

## Question 4
The dashboard JSON model is saved at:
`project_folder/dashboards/{filename}.json`. 

In my case, it is saved at:
`project_folder/dashboards/nyc_taxi_metrics_dashboard.json`

