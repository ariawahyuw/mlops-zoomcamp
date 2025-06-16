# Homework 4

## Question 1
_(in the jupyter notebook)_

> Answer: standard deviation of predicted duration is 6.24.

## Question 2
_(in the jupyter notebook)_

> Answer: the size of the output file is approximately 66MB.

## Question 3
Running:
```bash
jupyter nbconvert --to script starter.ipynb
```
will create `starter.py` in the directory.

## Question 4
By searching: `scikit-learn` in `Pipfile.lock`, we can see that it has:
```python
...
"hashes": [
    "sha256:057b991ac64b3e75c9c04b5f9395eaf19a6179244c089afdebaad98264bff37c",
    "sha256:118a8d229a41158c9f90093e46b3737120a165181a1b58c03461447aa4657415",
    "sha256:12e40ac48555e6b551f0a0a5743cc94cc5a765c9513fe708e01f0aa001da2801",
    ...
]
...
```
so the first hash for scikit-learn is:
`sha256:057b991ac64b3e75c9c04b5f9395eaf19a6179244c089afdebaad98264bff37c`

## Question 5
By running:
```bash
python starter.py 2023 4
```
the mean predicted duration for April 2023 can be found from the output:
```bash
y_pred mean: 14.292282936862449
y_pred standard deviation: 6.353996941249663
```
which is __14.29__.

## Question 6
By running:
```bash
docker build -t "ride
-duration-prediction:latest" .
```
and
```bash
docker run --rm ride-
duration-prediction:latest 2023 5
```
the mean predicted duration for May 2023 can be found from the output:
```bash
y_pred mean: 0.19174419265916945
y_pred standard deviation: 1.3881399472264797
```
which is __0.19__.