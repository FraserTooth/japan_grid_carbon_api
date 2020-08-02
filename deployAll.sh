#!/bin/bash

echo "Deploying All"
cd cloud_functions/api/

gcloud functions set-iam-policy --region=us-central1 api policy.json

sls deploy