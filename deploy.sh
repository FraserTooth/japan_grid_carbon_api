#!/bin/bash

STAGE=$1

if [ $STAGE == "staging" ]
then
    PROJECT="japan-grid-carbon-api-staging"
    ENDPOINT="apitest.denkicarbon.jp"
    SPECPATH="openapi-functions-staging.yaml"
elif [ $STAGE == "production" ]
then
    PROJECT="japan-grid-carbon-api"
    ENDPOINT="data.denkicarbon.jp"
    SPECPATH="openapi-functions-production.yaml"
else
    echo "No Stage Given"
    exit 1
fi

echo ">> Deploying All to $STAGE with project: $PROJECT"
cd cloud_functions/

# Set Policy for Main API to Unauthenticated
gcloud functions set-iam-policy --region=us-central1 api policy.json

echo ">> Deploying Cloud Function"
sls deploy --stage $STAGE --id $PROJECT

echo ">> Deploying API Spec"
gcloud endpoints services deploy $SPECPATH --project $PROJECT

CONFIG_ID=$(gcloud endpoints configs list --service $ENDPOINT --project $PROJECT | grep -oP -m 1 '^\d\d\d\d-\d\d-\S+')

echo ">> Building ESP-v2 Image"
../gcloud_build_image -s $ENDPOINT -c $CONFIG_ID -p $PROJECT

echo ">> Deploying ESP-v2 Image"
# Variable NEW_IMAGE is created during gcloud_build_image
gcloud run deploy api \
--image="$NEW_IMAGE" \
--set-env-vars=ESPv2_ARGS=--cors_preset=basic \
--allow-unauthenticated \
--platform managed \
--project=$PROJECT \
--region=us-central1