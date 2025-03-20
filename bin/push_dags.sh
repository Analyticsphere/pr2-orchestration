#!/bin/bash
# push_dags.sh: Pushes Airflow DAG files to your *dev* Composer environment.

set -e

# Configure for dev environment

# PROJECT_ID="nih-nci-dceg-connect-stg-5519"
# GCS_DAGS_FOLDER="gs://us-central1-ccc-jp-stg-c97d7d50-bucket/dags/"

# PROJECT_ID="nih-nci-dceg-connect-dev"
# GCS_DAGS_FOLDER="gs://us-central1-ccc-jp-dev-54b9b374-bucket/dags/"

PROJECT_ID="nih-nci-dceg-connect-prod-6d04"
GCS_DAGS_FOLDER="gs://us-central1-ccc-orchestrato-a82b22b0-bucket/dags/"

echo "Setting GCP project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

echo "Copying local DAGs to dev Composer bucket..."
<<<<<<< HEAD
gsutil -m cp -r dags/* $GCS_DAGS_FOLDER


echo "Done. DAGs have been pushed to dev environment!"
 