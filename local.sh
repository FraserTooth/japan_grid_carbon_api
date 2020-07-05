#!/bin/bash
COMPANY=$1
FUNCTION=$2
DIR="$PWD"

export GOOGLE_APPLICATION_CREDENTIALS="$DIR/gc-key.json"

echo "Locally Running Function $FUNCTION for $COMPANY"
cd cloud_functions/$COMPANY/scraper/

functions-framework --target $FUNCTION --debug

echo "Done"