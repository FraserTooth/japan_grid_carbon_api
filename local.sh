#!/bin/bash
yell() { echo "$0: $*" >&2; }
die() { yell "$*"; exit 111; }
try() { "$@" || die "cannot $*"; }

FUNCTION=$1
ENV=$2
DIR="$PWD"


if [ $ENV == 'production' ]
then
    export GCP_PROJECT="japan-grid-carbon-api"
elif [ $ENV == 'staging' ]
then
    export GCP_PROJECT="japan-grid-carbon-api-staging"
else
    echo "Not a valid environment - $ENV"
    exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="${HOME}/.gcloud/japan-grid-carbon-service-key-$ENV.json"
export STAGE=$ENV

echo "Locally Running Function $FUNCTION in $ENV"
cd cloud_functions/

functions-framework --target $FUNCTION --debug

echo "Done"