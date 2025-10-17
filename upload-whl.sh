#!/bin/sh

if [ ! -e ci/upload-whl.sh ] ; then
  echo "This script must be run from project root! Exiting."
  exit 1
fi

uv run aws s3 cp dist/cw_parq_converter*.whl s3://cw-data-team-artifacts/cw-parq-converter/

