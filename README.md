# Gemini Visual Waste Audit Tool

> **Note:** This is not an officially supported Google product. This project is
> intended for demonstration and reference purposes.

This project implements a serverless, event-driven pipeline on Google Cloud to
automate the analysis of large batches of images using the Gemini API.

The pipeline is designed for scalability and resilience, orchestrating Google
Cloud services like Cloud Run, Cloud Tasks, Firestore, BigQuery, and Eventarc to
create a robust image processing workflow.

## Architecture & Workflow

The pipeline consists of two main services: the **Ingestion Service** and the
**Processing Service**, both deployed on Cloud Run.

1.  **Trigger**: The pipeline is initiated when an image file (.jpg, .jpeg,
    .png) is uploaded to a designated **Google Cloud Storage (GCS)** bucket.
2.  **Event Notification**: An **Eventarc** trigger detects the new GCS object
    and sends a CloudEvent payload to the **Ingestion Service**.
3.  **Ingestion**: The Ingestion Service receives the GCS object details. For
    each prompt version defined in `utils/prompts.py`, it checks **Firestore**
    to see if the image has already been processed for that prompt.
4.  **Deduplication & Dispatch**: If a job for that image and prompt version is
    new or has failed, it creates an authenticated task in **Cloud Tasks** and
    marks the job as `QUEUED` in Firestore.
5.  **Processing**: The **Processing Service** picks up tasks from the Cloud
    Tasks queue. It updates the Firestore job status to `PROCESSING`.
6.  **Image Retrieval**: The service downloads the image file from GCS using the
    URI provided in the task.
7.  **AI Analysis**: The image is sent to the **Gemini API** along with the
    appropriate versioned prompt for analysis.
8.  **Storage**: If analysis is successful, the flattened JSON result from
    Gemini is written to a **BigQuery** table for results, and the Firestore job
    status is updated to `PROCESSED`. If analysis fails, the error is logged to
    a separate BigQuery table for errors, and the status is updated to `FAILED`
    in Firestore.
9.  **State Tracking**: **Firestore** is used throughout the process to track
    the state of each image-prompt job (e.g., `QUEUED`, `PROCESSING`,
    `PROCESSED`, `FAILED`).

## Directory Structure

-   `ingestion_service/`: Source code and Dockerfile for the Cloud Run service
    that ingests GCS events and dispatches tasks.
-   `processing_service/`: Source code and Dockerfile for the Cloud Run service
    that performs Gemini image analysis.
-   `utils/`: Shared Python modules for interacting with GCP services (GCS,
    BigQuery, Firestore, Gemini), logging configuration, and prompt definitions
    (`prompts.py`).
-   `deploy.sh`: A comprehensive bash script that automates infrastructure setup
    and deployment of services and triggers.

## Deployment

The entire infrastructure and application can be deployed by running
`deploy.sh`.

### Prerequisites

1.  Google Cloud SDK (`gcloud`) installed and authenticated.
2.  An active GCP project with billing enabled.
3.  Permissions to enable APIs, create service accounts, grant IAM roles, and
    deploy Cloud Run, Cloud Tasks, Eventarc, Firestore, and BigQuery resources.

### Steps

1.  **Make the script executable**: `chmod +x deploy.sh`
2.  **Run the script**:

    ```bash
    ./deploy.sh -p <YOUR_GCP_PROJECT_ID>
    ```

    The script accepts flags (`-p` for project ID, `-r` for region, `-b` for
    input bucket, `-s` for spreadsheet ID) or environment variables. If flags
    are omitted, it prompts interactively in terminal.

The script automates enabling required GCP APIs, creating service accounts,
setting IAM permissions, provisioning BigQuery tables and Firestore databases,
deploying both Cloud Run services, creating the Cloud Tasks queue, and
configuring the Eventarc storage trigger.

### Configuration via Google Sheets (Optional)

By default, the pipeline operates out-of-the-box using the built-in
configuration and material definitions in `utils/prompts.py`.

To dynamically customize prompts, model parameters, or waste material
classifications without redeploying code, you can connect a Google Spreadsheet:

1.  **Create your Google Spreadsheet**:
    -   In Google Drive, create a new blank Google Spreadsheet.
    -   For each CSV file in `config_template/` (`GeneralConfig.csv`,
        `ModelConfigs.csv`, and `Materials.csv`), navigate to **File > Import >
        Upload**, upload the file, and select **Insert new sheet(s)**.
    -   Ensure the sheet tab names match the CSV filenames exactly:
        `GeneralConfig`, `ModelConfigs`, and `Materials`.
2.  **Share with Service Accounts**:
    -   Share the spreadsheet with **Viewer** access to both Cloud Run service
        accounts created during deployment:
        -   Ingestion Service Account:
            `ingestion-service-sa@<PROJECT_ID>.iam.gserviceaccount.com`
        -   Processing Service Account:
            `processing-service-sa@<PROJECT_ID>.iam.gserviceaccount.com`
3.  **Deploy with Spreadsheet ID**:

    -   Pass your Spreadsheet ID during deployment:

        ```bash
        ./deploy.sh -p <PROJECT_ID> -s <SPREADSHEET_ID>
        ```
    -   Or set `export SPREADSHEET_ID="<SPREADSHEET_ID>"` before running

## Terms of Service

This project utilizes Google Cloud and Gemini API services. Use of these
services is subject to:

*   [Google Cloud Terms of Service](https://cloud.google.com/terms)
*   [Google APIs Terms of Service](https://developers.google.com/terms)

## Security & Vulnerability Reporting

Eligibility for the
[Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security)
is determined by the
[Google Open Source Software Vulnerability Reward Program Rules](https://bughunters.google.com/about/rules/open-source/google-open-source-software-vulnerability-reward-program-rules).

For instructions on reporting security vulnerabilities, please refer to
[SECURITY.md](SECURITY.md).

