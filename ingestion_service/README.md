# Ingestion Service

This service is a Gen 2 Google Cloud Function that acts as the entry point for
the Image Audit Pipeline.

## Role in the Pipeline

1.  **HTTP Trigger**: It is invoked by an authenticated HTTP POST request.
2.  **Read Image IDs**: It reads a list of unique image file IDs from a
    specific Google Sheet.
3.  **Deduplicate Jobs**: It checks against a Firestore collection to ensure
    that an image is not processed more than once for the same prompt version.
4.  **Dispatch Tasks**: For each new image, it creates a task with an OIDC
    authentication token and dispatches it to a Cloud Tasks queue, which then
    triggers the Processing Service.

## Key Components

-   `main.py`: The main Cloud Function code containing the HTTP request handler
    and the logic for task creation.
-   `Dockerfile`: Defines the container image for the Gen 2 Cloud Function.
-   `requirements.txt`: Lists the Python dependencies for the service.

All business logic modules (like reading from BigQuery) are located in the
top-level `utils/` directory.

## Environment Variables

This service requires the following environment variables, which are set
automatically by the main `deploy.sh` script:

-   `GCP_PROJECT_ID`: The Google Cloud Project ID.
-   `GCP_REGION`: The region where the services are deployed.
-   `TASK_QUEUE_ID`: The ID of the Cloud Tasks queue to dispatch tasks to.
-   `PROCESSING_SERVICE_URL`: The HTTPS URL of the deployed Processing Service.
-   `FIRESTORE_COLLECTION`: The name of the Firestore collection used for
    tracking job status.
-   `INGESTION_SA_EMAIL`: The email of this service's own service account, used
    to generate OIDC tokens for the tasks it creates.
