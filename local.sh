#!/bin/bash
PATH=$1
FUNCTION=$2
DIR="$PWD"

export GOOGLE_APPLICATION_CREDENTIALS="$DIR/gc-key.json"

echo "Locally Running Function $FUNCTION for $PATH"
cd cloud_functions/$PATH

functions-framework --target $FUNCTION --debug

echo "Done"