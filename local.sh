#!/bin/bash
FUNCTION=$1
DIR="$PWD"
export GCP_PROJECT="japan-grid-carbon-api"

export GOOGLE_APPLICATION_CREDENTIALS="$DIR/.gcloud/keyfile.json"

echo "Locally Running Function $FUNCTION"
cd cloud_functions/

functions-framework --target $FUNCTION --debug

echo "Done"