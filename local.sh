#!/bin/bash
FOLDER=$1
FUNCTION=$2
DIR="$PWD"
export GCP_PROJECT="japan-grid-carbon-api"

export GOOGLE_APPLICATION_CREDENTIALS="$DIR/gc-key.json"

echo "Locally Running Function $FUNCTION for $FOLDER"
cd cloud_functions/$FOLDER

functions-framework --target $FUNCTION --debug

echo "Done"