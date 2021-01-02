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


BASE_IMAGE_NAME="gcr.io/endpoints-release/endpoints-runtime-serverless"
ESP_TAG="2"
echo "Determining fully-qualified ESP version for tag: ${ESP_TAG}"
ALL_TAGS=$(gcloud container images list-tags "${BASE_IMAGE_NAME}" \
    --filter="tags~^${ESP_TAG}$" \
--format="value(tags)")
IFS=',' read -ra TAGS_ARRAY <<< "${ALL_TAGS}"

if [ ${#TAGS_ARRAY[@]} -eq 0 ]; then
error_exit "Did not find ESP version: ${ESP_TAG}"
fi;

# Find the tag with the longest length.
ESP_FULL_VERSION=""
for tag in "${TAGS_ARRAY[@]}"; do
    if [ ${#tag} -gt ${#ESP_FULL_VERSION} ]; then
    ESP_FULL_VERSION=${tag}
    fi
done
echo "Building image for ESP version: ${ESP_FULL_VERSION}"

echo ">> Deploying ESP-v2 Image"
gcloud run deploy api \
--image="gcr.io/$PROJECT/endpoints-runtime-serverless:${ESP_FULL_VERSION}-$ENDPOINT-$CONFIG_ID" \
--set-env-vars=ESPv2_ARGS=--cors_preset=basic \
--allow-unauthenticated \
--platform managed \
--project=$PROJECT \
--region=us-central1