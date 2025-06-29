# Homework 5

## Question 1
_Now we need to create the "main" block from which we'll invoke the main function. How does the if statement that we use for this looks like?_

> Answer: `if __name__ == __main__:`

## Question 2
*Next, create a folder tests and create two files. One will be the file with tests. We can name it test_batch.py.*

*What should be the other file?*
> Answer: `__init__.py`.

## Question 3
*How many rows should be there in the expected dataframe?*

> Answer: 2 rows, as written in `tests/test_batch.py` file.

## Question 4
...
*In both cases we should adjust commands for localstack. What option do we need to use for such purposes?*

> Answer: `--endpoint-url`, which we set as http://localhost:4566 for localstack.

## Question 5
*What's the size of the file? (dummy test dataframe for January 2023 in `integration-test/integration_test.py`)*

> Answer: `3620`, as printed by boto3 response from Localstack S3.


## Question 6
*What's the sum of predicted durations for the test dataframe?*

> Answer: 36.28, as printed from running `batch.py` using `INPUT_FILE_PATTERN` = `S3_URI` of test dataframe and sum all the prediction durations.
