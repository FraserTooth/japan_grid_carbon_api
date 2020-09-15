#!/bin/bash

echo "Deploying All"
cd cloud_functions/

# Set Policy for Main API to Unauthenticated
gcloud functions set-iam-policy --region=us-central1 api policy.json

sls deploy