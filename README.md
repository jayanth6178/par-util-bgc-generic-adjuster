# Parquet Converter
Parquet Converter is a data transformation tool to convert raw files from various data sources to parquet files at Code Willing


## Getting started

parquet_converter library is managed by [uv](https://docs.astral.sh/uv/). Please follow below steps to setup your dev environment

```
# install uv for the first time
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# sync up dev environment
$ cd {path_to_repo}/cw-parq-converter
$ uv sync
$ source .venv/bin/activate

# setup pre-commit hooks
$ pre-commit install
```

## Version control

Before pushing or pulling a version, ensure that you have a git.codewilling personal access token ready. Your token must have the `api` scope.

Throughout this tutorial, `{TOKEN}` represents your personal access token and `{version}` represents the version you want to push/pull.

### Pushing a version update:

Updating the version of the package is done with the following steps:

**Before merging to main**

1. Ensure all changes are committed and pushed to your branch.
2. Bump the version in `pyproject.toml`. This should be the LAST change you make before merging to main.

```
$ uv version --bump patch  # 1.0.0 -> 1.0.1
$ uv version --bump minor  # 1.0.0 -> 1.1.0
$ uv version --bump major  # 1.0.0 -> 2.0.0
```

3. Commit and push the version bump:

```
$ git add pyproject.toml
$ git commit -m "Bump version to {version}"
$ git push
```

4. Merge your branch into main.

**After merging to main**

5. Switch to main and pull the latest changes:

```
$ git checkout main
$ git pull
```

6. Tag the version in GitLab:

```
$ git tag -a v{version} -m "Release version {version}"
$ git push origin v{version}
```

You should now see your tag in the GitLab repo under Repository -> Tags. The GitLab CI pipeline will automatically build the wheel file and upload it to S3.

You can verify the build succeeded by checking the pipeline status for your tag in GitLab.


### Deployment:

To deploy a specific version to a production environment:

1. Download the wheel file from S3:

```
$ aws s3 cp --profile data_team_deploy s3://cw-data-team-artifacts/cw-parq-converter/cw_parq_converter-{version}-py3-none-any.whl /q/cw/deploy/cw-parq-converter/dist/
```

2. Create a new Python 3.12 virtual environment for this version:

```
$ cd /q/cw/deploy/cw-parq-converter/
$ uv venv envs/v{version} --python 3.12
```

3. Install the wheel file to the virtual environment:

```
$ uv pip install --python envs/v{version}/bin/python dist/cw_parq_converter-{version}-py3-none-any.whl
```

4. To use the new version, update your process/DAG to use the Python interpreter from the version-specific venv:

```
/q/cw/deploy/cw-parq-converter/envs/v{version}/bin/python
```

For example, if deploying version 1.1.0, your DAG should use:
```
/q/cw/deploy/cw-parq-converter/envs/v1.1.0/bin/python
```

### Pulling a specific version:

To install a specific version, use the following command: 
```
$ uv pip install cw-parq-converter=={version} --index-url https://__token__:{TOKEN}@git.codewilling.com/api/v4/projects/511/packages/pypi/simple
```

To add a specific version to your pyproject.toml, use the following command:
```
$ uv add cw-parq-converter=={version} --index-url https://__token__:{TOKEN}@git.codewilling.com/api/v4/projects/511/packages/pypi/simple
```