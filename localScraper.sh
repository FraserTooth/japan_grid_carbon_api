#!/bin/bash
COMPANY=$1
FUNCTION=$2
DIR="$PWD"
export GCP_PROJECT="japan-grid-carbon-api"

export GOOGLE_APPLICATION_CREDENTIALS="$DIR/.gcloud/keyfile.json"

echo "Locally Running Function $FUNCTION for $COMPANY"
cd cloud_functions/api/utilities/$COMPANY/scraper

functions-framework --target $FUNCTION --debug

echo "Done"
