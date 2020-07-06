#!/bin/bash
API=$1

echo "Deploying Function $API"
cd cloud_functions/api/

gcloud functions deploy $API --runtime python37 --trigger-http --allow-unauthenticated

echo "Done"