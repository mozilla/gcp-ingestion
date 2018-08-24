# GCP Ingestion Testplan

**NOTE:** For load test setup and instructions, see [loadtest.md](./loadtest.md).

@relud has some excellent CI testing docs for the [Edge Server](edge.md#testing) and [Validation Service](validate.md#testing) in this repository.

## Requirements

[Python 3.5+](https://www.python.org/downloads/) is required for the [PEP 484](https://www.python.org/dev/peps/pep-0484/) type hinting and for Molotov load tests.

## Lint

The code will use the Flake8 linter via pytest.

## Running tests

The CI tests will execute on the Circle-CI server for each PR.

If you want to run the tests locally before submitting a PR, you can do the following steps:

1. stuff
1. things
