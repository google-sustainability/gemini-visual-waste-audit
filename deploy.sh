#!/bin/bash
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==============================================================================
# Comprehensive Deployment Script for Image Audit GCP Services (Event-Driven)
# ==============================================================================
#
# This script automates the entire setup and deployment process, including:
# 1. Enabling required GCP APIs.
# 2. Creating Service Accounts.
# 3. Granting all necessary IAM permissions.
# 4. Creating the BigQuery dataset and table.
# 5. Deploying the Processing Service to Google Cloud Run.
# 6. Deploying the Ingestion Service to Google Cloud Run.
# 7. Creating Eventarc Trigger for GCS.
#
# Fill in the variables in the "CONFIGURATION" section below.
#

# --- Bash Safety Settings ---
set -e
set -u
set -o pipefail

# --- CONFIGURATION (Environment variables or CLI arguments) ---
# Default values can be overridden via environment variables or command-line flags.
GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
GCP_REGION="${GCP_REGION:-us-central1}"
GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-}"
BIGQUERY_DATASET="${BIGQUERY_DATASET:-waste_audit_dataset}"
BIGQUERY_MATERIALS_TABLE="${BIGQUERY_MATERIALS_TABLE:-gemini_waste_audit_materials}"
BIGQUERY_ERRORS_TABLE="${BIGQUERY_ERRORS_TABLE:-gemini_waste_audit_errors}"
FIRESTORE_COLLECTION="${FIRESTORE_COLLECTION:-image_audit_jobs}"
SPREADSHEET_ID="${SPREADSHEET_ID:-}"
TASK_QUEUE_ID="${TASK_QUEUE_ID:-image-audit-queue}"

# Parse optional command-line flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project_id|-p)
      GCP_PROJECT_ID="$2"
      shift 2
      ;;
    --region|-r)
      GCP_REGION="$2"
      shift 2
      ;;
    --bucket|-b)
      GCS_BUCKET_NAME="$2"
      shift 2
      ;;
    --spreadsheet_id|-s)
      SPREADSHEET_ID="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  -p, --project_id       GCP Project ID (required)"
      echo "  -r, --region           GCP Region (default: us-central1)"
      echo "  -b, --bucket           GCS Bucket to monitor (default: <project-id>-waste-audit-images)"
      echo "  -s, --spreadsheet_id   Google Sheets ID for prompt configuration (optional)"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# Interactive fallback if GCP_PROJECT_ID is not provided
if [[ -z "${GCP_PROJECT_ID}" ]]; then
  if [[ -t 0 ]]; then
    read -rp "Enter GCP Project ID: " GCP_PROJECT_ID
  fi
fi

if [[ -z "${GCP_PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is required. Set via environment variable, --project_id flag, or interactive prompt." >&2
  exit 1
fi

if [[ -z "${GCS_BUCKET_NAME}" ]]; then
  GCS_BUCKET_NAME="${GCP_PROJECT_ID}-waste-audit-images"
fi

export GCP_PROJECT_ID
export GCP_REGION
export GCS_BUCKET_NAME
export GCS_THUMBNAIL_BUCKET_NAME="${GCS_BUCKET_NAME}-thumbnails"
export BIGQUERY_DATASET
export BIGQUERY_MATERIALS_TABLE
export BIGQUERY_ERRORS_TABLE
export FIRESTORE_COLLECTION
export SPREADSHEET_ID
export TASK_QUEUE_ID

# --- DERIVED VARIABLES (Do not change) ---
PROCESSING_SA_NAME="processing-service-sa"
INGESTION_SA_NAME="ingestion-service-sa"
PROCESSING_SA_EMAIL="${PROCESSING_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
INGESTION_SA_EMAIL="${INGESTION_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
INGESTION_SERVICE_NAME="ingestion-service"
PROCESSING_SERVICE_NAME="image-processing-service"

# --- SCRIPT LOGIC ---

echo "--- Starting Full Infrastructure and Application Deployment ---"
echo "Project: ${GCP_PROJECT_ID}"
echo "Region: ${GCP_REGION}"

# --- Step 1: Enable GCP APIs ---
echo -e "\n[1/10] Enabling required GCP APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com \
  cloudtasks.googleapis.com \
  cloudscheduler.googleapis.com \
  firestore.googleapis.com \
  bigquery.googleapis.com \
  aiplatform.googleapis.com \
  artifactregistry.googleapis.com \
  eventarc.googleapis.com \
  storage.googleapis.com \
  sheets.googleapis.com \
  drive.googleapis.com \
  --project="${GCP_PROJECT_ID}"

# Pre-initialize Service Agents to mitigate propagation delays in subsequent steps
gcloud beta services identity create --service=eventarc.googleapis.com --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || true
gcloud beta services identity create --service=storage.googleapis.com --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || true

# --- Step 2: Create Service Accounts ---
echo -e "\n[2/10] Creating Service Accounts (if they don't exist)..."
# Create Processing Service Account
if ! gcloud iam service-accounts describe "${PROCESSING_SA_EMAIL}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    gcloud iam service-accounts create "${PROCESSING_SA_NAME}" \
      --display-name="Service Account for Image Processing" \
      --project="${GCP_PROJECT_ID}"
else
    echo "Service Account '${PROCESSING_SA_NAME}' already exists."
fi
# Create Ingestion Service Account
if ! gcloud iam service-accounts describe "${INGESTION_SA_EMAIL}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    gcloud iam service-accounts create "${INGESTION_SA_NAME}" \
      --display-name="Service Account for Task Ingestion" \
      --project="${GCP_PROJECT_ID}"
else
    echo "Service Account '${INGESTION_SA_NAME}' already exists."
fi

# --- Step 3: Grant IAM Permissions ---
echo -e "\n[3/10] Granting IAM permissions..."
# Permissions for Processing Service
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/aiplatform.user" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/bigquery.dataEditor" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/bigquery.jobUser" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/datastore.user" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/logging.logWriter" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/storage.objectAdmin" \
  --condition=None # Needed to read and upload GCS objects & thumbnails
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --role="roles/run.invoker" \
  --condition=None
gcloud iam service-accounts add-iam-policy-binding "${PROCESSING_SA_EMAIL}" \
  --role="roles/iam.serviceAccountTokenCreator" \
  --member="serviceAccount:${PROCESSING_SA_EMAIL}" \
  --project="${GCP_PROJECT_ID}"

# Permissions for Ingestion Service
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --role="roles/cloudtasks.enqueuer" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --role="roles/datastore.user" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --role="roles/logging.logWriter" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --role="roles/eventarc.eventReceiver" \
  --condition=None
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --role="roles/run.invoker" \
  --condition=None

# Allow Ingestion SA to impersonate itself (for Cloud Tasks OIDC)
gcloud iam service-accounts add-iam-policy-binding "${INGESTION_SA_EMAIL}" \
  --role="roles/iam.serviceAccountUser" \
  --member="serviceAccount:${INGESTION_SA_EMAIL}" \
  --project="${GCP_PROJECT_ID}"

# Grant Pub/Sub Publisher to GCS Service Account (Required for Eventarc GCS triggers)
echo "Ensuring GCS input and thumbnail buckets exist..."
for BUCKET in "${GCS_BUCKET_NAME}" "${GCS_THUMBNAIL_BUCKET_NAME}"; do
    if ! gcloud storage buckets describe "gs://${BUCKET}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
        echo "Creating bucket '${BUCKET}'..."
        gcloud storage buckets create "gs://${BUCKET}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
    else
        echo "Bucket '${BUCKET}' already exists."
    fi
done

echo "Ensuring GCS Service Account has Pub/Sub Publisher role..."
GCS_SA="$(gcloud storage service-agent --project="${GCP_PROJECT_ID}" 2>/dev/null | tr -d '[:space:]')"
if [[ -z "${GCS_SA}" ]]; then
    PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format="value(projectNumber)")"
    GCS_SA="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"
fi

for i in {1..5}; do
    if gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
        --member="serviceAccount:${GCS_SA}" \
        --role="roles/pubsub.publisher" \
        --condition=None >/dev/null 2>&1; then
        echo "Granted roles/pubsub.publisher to GCS Service Account (${GCS_SA})."
        break
    fi
    echo "Attempt $i failed to bind Pub/Sub Publisher to GCS Service Account. Retrying in 5s..."
    sleep 5
done

echo "IAM roles granted."

# --- Step 4: Create BigQuery Resources ---
echo -e "\n[4/10] Creating BigQuery Dataset and Table (if they don't exist)..."
if ! bq show --dataset "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}" >/dev/null 2>&1;
then
    bq mk --dataset "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}"
    echo "Dataset '${BIGQUERY_DATASET}' created."
else
    echo "Dataset '${BIGQUERY_DATASET}' already exists."
fi

# Create Waste Audit Materials Table
if ! bq show "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_MATERIALS_TABLE}" >/dev/null 2>&1;
then
    bq mk --table \
      "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_MATERIALS_TABLE}" \
      'image_id:STRING,model_name:STRING,prompt_version_id:STRING,processing_timestamp:TIMESTAMP,gemini_result:JSON,class:STRING,material:STRING,estimated_percentage:FLOAT,scaled_weight:FLOAT,gcs_uri:STRING,gcs_thumb_uri:STRING,raw_signed_url:STRING,thumb_signed_url:STRING'
    echo "Table '${BIGQUERY_MATERIALS_TABLE}' created."
else
    echo "Table '${BIGQUERY_MATERIALS_TABLE}' already exists."
fi

# Idempotent schema migration for existing BigQuery tables
echo "Ensuring BigQuery table schema includes Signed URL columns..."
bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" \
  "ALTER TABLE \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET}.${BIGQUERY_MATERIALS_TABLE}\`
   ADD COLUMN IF NOT EXISTS gcs_thumb_uri STRING,
   ADD COLUMN IF NOT EXISTS raw_signed_url STRING,
   ADD COLUMN IF NOT EXISTS thumb_signed_url STRING;" >/dev/null 2>&1 || true

# Create Waste Audit Errors Table
if ! bq show "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_ERRORS_TABLE}" >/dev/null 2>&1;
then
    bq mk --table \
      "${GCP_PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_ERRORS_TABLE}" \
      'image_id:STRING,model_name:STRING,prompt_version_id:STRING,error_timestamp:TIMESTAMP,error_message:STRING,gcs_uri:STRING'
    echo "Table '${BIGQUERY_ERRORS_TABLE}' created."
else
    echo "Table '${BIGQUERY_ERRORS_TABLE}' already exists."
fi

# --- Step 5: Create Firestore Database ---
echo -e "\n[5/10] Creating Firestore database (if it doesn't exist)..."
if ! gcloud firestore databases describe --project="${GCP_PROJECT_ID}" --format="value(name)" | grep -q "(default)"; then
    gcloud firestore databases create --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
else
    echo "Firestore database already exists."
fi


# --- Step 6: Deploy the Processing Service to Cloud Run ---
echo -e "\n[6/10] Deploying the Processing Service to Cloud Run..."
# Temporarily copy utils directory
echo "Copying utils directory to processing_service..."
cp -r utils processing_service/

trap 'rm -rf processing_service/utils' EXIT

gcloud run deploy "${PROCESSING_SERVICE_NAME}" \
  --source "./processing_service" \
  --region "${GCP_REGION}" \
  --service-account "${PROCESSING_SA_EMAIL}" \
  --no-allow-unauthenticated \
  --max-instances=5 \
  --concurrency=3 \
  --cpu=1 \
  --memory=2Gi \
  --set-env-vars "GCP_PROJECT_ID=${GCP_PROJECT_ID}" \
  --set-env-vars "BIGQUERY_DATASET=${BIGQUERY_DATASET}" \
  --set-env-vars "BIGQUERY_TABLE=${BIGQUERY_MATERIALS_TABLE}" \
  --set-env-vars "BIGQUERY_ERRORS_TABLE=${BIGQUERY_ERRORS_TABLE}" \
  --set-env-vars "FIRESTORE_COLLECTION=${FIRESTORE_COLLECTION}" \
  --set-env-vars "SPREADSHEET_ID=${SPREADSHEET_ID}" \
  --set-env-vars "PROCESSING_SA_EMAIL=${PROCESSING_SA_EMAIL}" \
  --project="${GCP_PROJECT_ID}" \
  --quiet

rm -rf processing_service/utils
trap - EXIT

PROCESSING_SERVICE_URL=$(gcloud run services describe "${PROCESSING_SERVICE_NAME}" --region "${GCP_REGION}" --format 'value(status.url)' --project="${GCP_PROJECT_ID}")
echo "Processing Service deployed: ${PROCESSING_SERVICE_URL}"

# --- Step 7: Create the Cloud Tasks Queue ---
echo -e "\n[7/10] Creating Cloud Tasks Queue..."
if ! gcloud tasks queues describe "${TASK_QUEUE_ID}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    gcloud tasks queues create "${TASK_QUEUE_ID}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --max-attempts=5 \
      --min-backoff=10s \
      --max-backoff=120s \
      --max-concurrent-dispatches=60 \
      --max-dispatches-per-second=60
    echo "Queue '${TASK_QUEUE_ID}' created."
else
    echo "Queue '${TASK_QUEUE_ID}' already exists."
fi

# --- Step 8: Deploy the Ingestion Service to Cloud Run ---
echo -e "\n[8/10] Deploying the Ingestion Service to Cloud Run..."
# Temporarily copy utils directory
echo "Copying utils directory to ingestion_service..."
cp -r utils ingestion_service/

trap 'rm -rf ingestion_service/utils' EXIT

gcloud run deploy "${INGESTION_SERVICE_NAME}" \
  --source "./ingestion_service" \
  --region "${GCP_REGION}" \
  --service-account "${INGESTION_SA_EMAIL}" \
  --no-allow-unauthenticated \
  --max-instances=5 \
  --concurrency=10 \
  --cpu=1 \
  --memory=2Gi \
  --set-env-vars "GCP_PROJECT_ID=${GCP_PROJECT_ID}" \
  --set-env-vars "GCP_REGION=${GCP_REGION}" \
  --set-env-vars "TASK_QUEUE_ID=${TASK_QUEUE_ID}" \
  --set-env-vars "PROCESSING_SERVICE_URL=${PROCESSING_SERVICE_URL}" \
  --set-env-vars "FIRESTORE_COLLECTION=${FIRESTORE_COLLECTION}" \
  --set-env-vars "INGESTION_SA_EMAIL=${INGESTION_SA_EMAIL}" \
  --set-env-vars "SPREADSHEET_ID=${SPREADSHEET_ID}" \
  --project="${GCP_PROJECT_ID}" \
  --quiet

rm -rf ingestion_service/utils
trap - EXIT

INGESTION_SERVICE_URL=$(gcloud run services describe "${INGESTION_SERVICE_NAME}" --region "${GCP_REGION}" --format 'value(status.url)' --project="${GCP_PROJECT_ID}")
echo "Ingestion Service deployed: ${INGESTION_SERVICE_URL}"

# --- Step 9: Create Eventarc Trigger ---
echo -e "\n[9/10] Creating Eventarc Trigger..."

TRIGGER_NAME="waste-audit-gcs-trigger"

# Ensure bucket exists (Optional, you might want to create it if not exists)
if ! gcloud storage buckets describe "gs://${GCS_BUCKET_NAME}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    echo "Creating bucket '${GCS_BUCKET_NAME}'..."
    gcloud storage buckets create "gs://${GCS_BUCKET_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
fi

if ! gcloud eventarc triggers describe "${TRIGGER_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    echo "Creating Eventarc trigger '${TRIGGER_NAME}'..."
    for i in {1..6}; do
        if gcloud eventarc triggers create "${TRIGGER_NAME}" \
            --location="${GCP_REGION}" \
            --destination-run-service="${INGESTION_SERVICE_NAME}" \
            --destination-run-region="${GCP_REGION}" \
            --destination-run-path="/ingest" \
            --event-filters="type=google.cloud.storage.object.v1.finalized" \
            --event-filters="bucket=${GCS_BUCKET_NAME}" \
            --service-account="${INGESTION_SA_EMAIL}" \
            --project="${GCP_PROJECT_ID}"; then
            echo "Eventarc trigger '${TRIGGER_NAME}' created."
            break
        fi
        if [[ "$i" -lt 6 ]]; then
            echo "Attempt $i failed. Eventarc Service Agent / IAM permissions may still be propagating. Retrying in 15 seconds..."
            sleep 15
        else
            echo "Failed to create Eventarc trigger after 6 attempts."
            exit 1
        fi
    done
else
    echo "Eventarc trigger '${TRIGGER_NAME}' already exists."
fi

# --- Step 10: Create Cloud Scheduler Job for Signed URL Refresher ---
echo -e "\n[10/10] Creating Cloud Scheduler Job for Signed URL Refresher (Every 5 Days)..."

REFRESH_JOB_NAME="waste-audit-refresh-schedule"

if ! gcloud scheduler jobs describe "${REFRESH_JOB_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1;
then
    gcloud scheduler jobs create http "${REFRESH_JOB_NAME}" \
      --location="${GCP_REGION}" \
      --schedule="0 2 */5 * *" \
      --uri="${PROCESSING_SERVICE_URL}/refresh-urls" \
      --http-method=POST \
      --oidc-service-account-email="${PROCESSING_SA_EMAIL}" \
      --oidc-token-audience="${PROCESSING_SERVICE_URL}" \
      --project="${GCP_PROJECT_ID}"
    echo "Cloud Scheduler job '${REFRESH_JOB_NAME}' created."
else
    echo "Cloud Scheduler job '${REFRESH_JOB_NAME}' already exists."
fi

echo -e "\n--- Deployment Complete ---"
echo "Upload images to gs://${GCS_BUCKET_NAME} to trigger the pipeline."
if [[ -n "${SPREADSHEET_ID}" ]]; then
    echo -e "\n[IMPORTANT] ACTION REQUIRED FOR CONFIG SYNC:"
    echo "To enable prompt synchronization from Google Sheets, you must share the spreadsheet with BOTH Cloud Run service accounts:"
    echo "  1. Open the Google Sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}"
    echo "  2. Click the 'Share' button in the top right."
    echo "  3. Add the following Service Account emails (with 'Viewer' access):"
    echo "     - Ingestion Service:  ${INGESTION_SA_EMAIL}"
    echo "     - Processing Service: ${PROCESSING_SA_EMAIL}"
    echo "Note: If not shared, the services will automatically fall back to the local prompts.py config."
fi
