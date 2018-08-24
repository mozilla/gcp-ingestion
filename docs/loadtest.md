# GCP Ingestion Load Testing

## Requirements

- [Python 3.5+](https://www.python.org/downloads/)
- [Molotov](https://github.com/loads/molotov/) ([Docs](https://molotov.readthedocs.io/))
- [virtualenv](https://virtualenv.pypa.io/)

## Installation

```sh
$ virtualenv venv -p python3
$ source ./venv/bin/activate
$ pip install -r requirements.txt
```

## Usage

```sh
$ molotov -h
$ molotov -cvv --max-runs 1
```

## Targets

See [Edge Server Pre-deployment Testing](./edge.md#pre-deployment-testing) and [Validation Server Pre-deployment Testing](./validate.md#pre-deployment-testing) for specific load details.

---

@rpapa and I are also investigating using something like [locust.io](https://locust.io) as well as bringing this up to scale using Kubernetes (versus our existing Ardere tool, which heavily uses AWS lambdas). Although we're considering Kubernetes regardless of whether we stick w/ Molotov or go towards Locust.
