# Ingestion Service

This Cloud Run service acts as the Eventarc entry point for the Gemini Visual
Waste Audit pipeline, ingesting and reconciling GCS Object Finalized (`/ingest`)
events.

## Workflow

1.  **Validate & Fan Out**: `POST /ingest` validates incoming Eventarc GCS
    metadata (`bucket`, `name`, and optional positive 64-bit `generation`),
    skips unsupported file extensions, and fans out across configured prompt
    versions (loaded dynamically from Google Sheets via `ConfigManager` when
    `SPREADSHEET_ID` is set, or falling back to `utils/prompts.py`).
2.  **Idempotency & In-Flight Deferral**: Compares the incoming GCS `generation`
    against the existing `(gcs_uri, prompt_version_id)` Firestore job document.
    Duplicate or older object generations are skipped idempotently (`200 OK`);
    newer generations arriving while a prior job is still in flight (`QUEUED`,
    `PROCESSING`, or active retry backoff) return `503` so Eventarc retries once
    the active worker settles.
3.  **Overwrite Cleanup & Task Dispatch**: Atomically transitions the Firestore
    job document to `QUEUED` using `last_update_time` preconditions. On settled
    overwrites, `/ingest` hard-deletes superseded BigQuery rows for that
    `(gcs_uri, prompt_version_id)`—preserving rows produced by other prompt
    versions—before creating an OIDC-authenticated task in Cloud Tasks, rolling
    back the Firestore claim if BigQuery deletion or task creation fails.

## Key Components

-   `main.py`: Flask application handling health checks (`GET /`) and Eventarc
    ingestion requests (`POST /ingest`).
-   `job_store.py`: Firestore job lifecycle state machine and `last_update_time`
    optimistic-concurrency claim/rollback helpers.
-   `bigquery_reconciler.py`: Parameterized BigQuery DML helpers for deleting
    superseded audit rows with serialization-conflict retries.
-   `Dockerfile`: Container image definition running Gunicorn on Cloud Run.
-   `requirements.txt`: Python dependencies for the service.

Shared modules (prompt definitions, dynamic Sheets configuration, and logging
setup) live in the top-level `utils/` directory.

## Environment Variables

This service uses the following environment variables, configured automatically
by `deploy.sh`:

-   `GCP_PROJECT_ID`: Google Cloud project ID.
-   `GCP_REGION`: Region hosting the Cloud Run service and Cloud Tasks queue.
-   `TASK_QUEUE_ID`: Cloud Tasks queue ID used to dispatch processing tasks.
-   `PROCESSING_SERVICE_URL`: HTTPS URL of the deployed Processing Service.
-   `FIRESTORE_COLLECTION`: Firestore collection tracking job status (defaults
    to `image_audit_jobs`).
-   `INGESTION_SA_EMAIL`: Service account email used to mint OIDC tokens on
    Cloud Tasks HTTP requests.
-   `BIGQUERY_DATASET`: BigQuery dataset containing audit result rows (required
    when deleting superseded rows on overwrites).
-   `BIGQUERY_TABLE`: BigQuery table containing audit result rows (required when
    deleting superseded rows on overwrites).
-   `SPREADSHEET_ID`: Optional Google Spreadsheet ID for dynamic prompt and
    model configuration.
