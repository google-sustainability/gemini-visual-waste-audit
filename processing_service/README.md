# Processing Service

This service is a Google Cloud Run application that processes images using the
Gemini API.

## Role in the Pipeline

1.  **Trigger**: It is invoked by a task from a Cloud Tasks queue via an
    authenticated HTTP POST request.
2.  **Image Retrieval**: It downloads an image from Google Cloud Storage based
    on the `gcs_uri` received in the task payload.
3.  **AI Analysis**: It sends the image to the Gemini API for analysis based on
    a versioned prompt.
4.  **Storage**: It writes the flattened analysis results to the
    `gemini_waste_audit_materials` BigQuery table. If analysis fails, it writes
    to the `gemini_waste_audit_errors` table.
5.  **State Tracking**: It updates the job status in Firestore to 'completed' or
    'error'.

## Key Components

-   `main.py`: The main Flask application code containing the HTTP request
    handler and the logic for image processing.
-   `Dockerfile`: Defines the container image for the Cloud Run service.
-   `requirements.txt`: Lists the Python dependencies for the service.

All business logic modules (like downloading from GCS, processing with Gemini,
and writing to BigQuery) are located in the top-level `utils/` directory.

## Environment Variables

This service requires the following environment variables, which are set
automatically by the main `deploy.sh` script:

-   `GCP_PROJECT_ID`: The Google Cloud Project ID.
-   `BIGQUERY_DATASET`: The BigQuery dataset where results are stored.
-   `BIGQUERY_TABLE`: The BigQuery table for successful analysis results.
-   `BIGQUERY_ERRORS_TABLE`: The BigQuery table for logging processing errors.
-   `FIRESTORE_COLLECTION`: The name of the Firestore collection used for
    tracking job status.
