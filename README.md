# catflow-service-videosplit

Consumer/publisher loop for workers in an object recognition pipeline

# Setup

* Install [pre-commit](https://pre-commit.com/#install) in your virtualenv. Run
`pre-commit install` after cloning this repository.

# Develop

```
pip install --editable .[dev]
```

# Build

```
pip install build
python -m build
docker build -t iank1/catflow_service_videosplit:latest .
```

# Test

```
pytest
```

# Deploy

See [catflow-docker](https://github.com/iank/catflow-docker) for `docker-compose.yml`
