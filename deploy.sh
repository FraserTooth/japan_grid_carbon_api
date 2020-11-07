#!/bin/bash

STAGE=$1

if [ $STAGE == "staging" ]
then
    PROJECT="japan-grid-carbon-api-staging"
    ENDPOINT="apitest.denkicarbon.jp"
    SPECPATH="openapi-functions-staging.yaml"
elif [ $STAGE == "production" ]
then
    ENDPOINT="api.denkicarbon.jp"
    SPECPATH="openapi-functions.yaml"
    PROJECT="japan-grid-carbon-api"
else
    echo "No stage Given"
    exit 1
fi

echo "Deploying All on $STAGE"
cd cloud_functions/

# Set Policy for Main API to Unauthenticated
gcloud functions set-iam-policy --region=us-central1 api policy.json

sls deploy --stage $STAGE --id $PROJECT

gcloud endpoints services deploy cloud_functions/$SPECPATH \
--project $PROJECT

CONFIG_ID=$(gcloud endpoints configs list --service $ENDPOINT | grep -oP -m 1 '^\d\d\d\d-\d\d-\S+')

./gcloud_build_image -s $ENDPOINT -c $CONFIG_ID -p $PROJECT

gcloud run deploy api \
--image="gcr.io/$PROJECT/endpoints-runtime-serverless:2.20.0-$ENDPOINT-$CONFIG_ID" \
--set-env-vars=ESPv2_ARGS=--cors_preset=basic \
--allow-unauthenticated \
--platform managed \
--project=$PROJECT