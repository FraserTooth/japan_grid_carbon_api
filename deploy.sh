#!/bin/bash

STAGE=$1
ID=$2

echo "Deploying All on $STAGE"
cd cloud_functions/

# Set Policy for Main API to Unauthenticated
gcloud functions set-iam-policy --region=us-central1 api policy.json

sls deploy --stage $STAGE --id $ID