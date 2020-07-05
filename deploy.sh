#!/bin/bash
FUNCTION=$1

echo "Deploying Function $1"
cd cloud_functions/scrapers/$1/

gcloud functions deploy $1 --runtime python37 --trigger-http --allow-unauthenticated

echo "Done"