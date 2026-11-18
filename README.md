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
3.  **Ingestion & Generation Reconciliation**: The Ingestion Service (`/ingest`)
    validates incoming GCS metadata (`bucket`, `name`, and optional
    `generation`) and fans out across configured prompt versions (loaded from
    Google Sheets when configured, or falling back to `utils/prompts.py`).
    Duplicate or older object generations are skipped idempotently; if a newer
    generation arrives while a prior job is still in flight, `/ingest` returns
    `503` so Eventarc retries once the active worker settles.
4.  **Overwrite Cleanup & Dispatch**: For new uploads, settled retries, or
    settled overwrites, `/ingest` atomically claims the
    `(gcs_uri, prompt_version_id)` job record as `QUEUED` in **Firestore** using
    `last_update_time` preconditions. On settled overwrites, it hard-deletes
    superseded **BigQuery** rows for that
    `(gcs_uri, prompt_version_id)`—preserving rows from other prompt
    versions—before creating an authenticated task in **Cloud Tasks** (rolling
    back the Firestore claim if BigQuery cleanup or task dispatch fails).
5.  **Processing**: The **Processing Service** picks up tasks from the Cloud
    Tasks queue. It updates the Firestore job status to `PROCESSING`.
6.  **Image Retrieval**: The service downloads the image file from GCS using the
    URI provided in the task.
7.  **AI Analysis**: The image is sent to the **Gemini API** along with the
    appropriate versioned prompt for analysis.
8.  **Storage**: If analysis is successful, the flattened JSON result from
    Gemini is written to a **BigQuery** table for results, and the Firestore job
    status is updated to `PROCESSED`. If analysis fails, the error is logged to
    a separate BigQuery table for errors, and the status is updated to
    `FAILED_RETRY` or `FAILED_PERMANENT` in Firestore.
9.  **State Tracking**: **Firestore** is used throughout the process to track
    the state of each `(gcs_uri, prompt_version_id)` job (`QUEUED`,
    `PROCESSING`, `PROCESSED`, `FAILED_RETRY`, `FAILED_PERMANENT`, `DELETING`).
10. **Object Deletion**: When an image is deleted from GCS, an Eventarc trigger
    notifies `/delete` on the **Ingestion Service**, which defers (`503`) if any
    job is still in flight, skips retired generations if a newer live object
    exists, purges all **BigQuery** rows for `gcs_uri`, and deletes settled
    **Firestore** job documents so re-uploads run as fresh uploads.

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

## Privacy & Data Retention

> **Disclaimer:** The information below summarizes how this reference
> architecture interacts with Google Cloud and Gemini services. It is provided
> for informational purposes only and does not constitute legal advice. Always
> review the linked official terms and consult your organization's legal,
> privacy, and compliance teams to ensure your deployment meets your regulatory
> and procurement requirements.

### Self-Hosted Deployment in Your Own GCP Project

This pipeline is fully **self-hosted** inside your organization's own Google
Cloud project (`<YOUR_GCP_PROJECT_ID>`). When you run `deploy.sh`, all compute,
queuing, storage, and database resources are provisioned directly within your
project and chosen Google Cloud region (default `us-central1`). No uploaded
images, prompts, or audit outputs are transmitted to the maintainers of this
open-source repository or to any external third party.

### Gemini API & Vertex AI Data Governance

By default, the Processing Service calls Gemini models through **Vertex AI**
within your GCP project:

-   **No Model Training on Customer Data**: Under the
    [Google Cloud Service Specific Terms (Training Restriction)](https://cloud.google.com/terms/service-terms)
    and
    [Vertex AI Data Governance](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance),
    Google does **not** use customer data—including uploaded waste audit images,
    prompts, or generated JSON responses—to train or fine-tune Google's
    foundation models. If you adapt the code to use a paid Gemini Developer API
    key (`ai.google.dev`), the
    [Gemini API Additional Terms of Service (Paid Services)](https://ai.google.dev/gemini-api/terms#paid-services)
    similarly state that Google doesn't use your prompts or responses to improve
    its products, and data processing is governed by the
    [Cloud Data Processing Addendum (CDPA)](https://cloud.google.com/terms/data-processing-addendum).
-   **Encryption**: Customer data is encrypted in transit and at rest by default
    across Google Cloud Storage, Firestore, BigQuery, and Vertex AI
    ([Encryption at Rest](https://cloud.google.com/docs/security/encryption-at-rest),
    [Encryption in Transit](https://cloud.google.com/docs/security/encryption-in-transit)).
-   **Transient Caching, Abuse Monitoring, and Zero Data Retention (ZDR)**:
    Vertex AI does not persistently store your prompts or model responses for
    product improvement, but two platform-level behaviors apply by default as
    documented in
    [Vertex AI Data Governance](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance):
    1.  **In-Memory Data Caching**: Vertex AI may cache customer inputs and
        outputs in memory for up to 24 hours (isolated to your GCP project) to
        reduce latency on repeated requests. Project administrators can disable
        this cache at the project level; see
        [Data caching](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance#caching).
    2.  **Abuse Monitoring**: For projects governed by the standard Google Cloud
        Terms of Service without an Invoiced Cloud Billing account, prompts may
        be temporarily logged for safety and abuse policy monitoring.
        Organizations that require strict **Zero Data Retention (ZDR)** on
        Vertex AI can disable data caching and request an abuse-monitoring
        logging exception as described in
        [Zero data retention](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance#zero-data-retention)
        and
        [Abuse monitoring](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance#abuse-monitoring).
        (This pipeline does not enable Grounding with Google Search.)

### What the Pipeline Stores in Your GCP Project

All application data persisted by the pipeline resides in resources you own and
control inside your GCP project:

-   **Cloud Storage — Raw Input Bucket (`<PROJECT_ID>-waste-audit-images` or
    custom `-b` bucket)**: Stores the original uploaded waste audit images
    (`.jpg`, `.jpeg`, `.png`).
-   **Cloud Storage — Thumbnail Bucket (`<GCS_BUCKET_NAME>-thumbnails`)**:
    Stores compressed `200x200` WebP thumbnails
    (`thumbnails/<image_id>_thumb.webp`) generated by the Processing Service for
    lightweight visualization and dashboarding.
-   **BigQuery (`waste_audit_dataset`)**:
    -   `gemini_waste_audit_materials`: Stores flattened material composition
        estimates (`percentage`, `scaled_weight`), scene descriptions
        (`overall_description`, `contamination_notes`), the full JSON model
        response (`gemini_result`), GCS URIs (`gcs_uri`, `gcs_thumb_uri`), and
        7-day V4 Signed URLs (`raw_signed_url`, `thumb_signed_url`) that are
        automatically refreshed every 5 days by a Cloud Scheduler job
        (`waste-audit-refresh-schedule` calling `POST /refresh-urls`).
    -   `gemini_waste_audit_errors`: Stores processing failure records
        (`image_id`, `model_name`, `prompt_version_id`, `error_timestamp`,
        `error_message`, `gcs_uri`).
-   **Firestore (`image_audit_jobs` collection in `(default)` database)**:
    Stores job deduplication and state records keyed by the SHA-256 hash of
    `<gcs_uri>:<prompt_version_id>`, tracking `status` (`QUEUED`, `PROCESSING`,
    `PROCESSED`, `FAILED_RETRY`, `FAILED_PERMANENT`), retry `attempts`, and
    timestamps.
-   **Cloud Tasks & Cloud Logging**: `image-audit-queue` holds transient task
    payloads while jobs are in flight, and Cloud Run services emit operational
    logs to Cloud Logging in your project.
-   **Google Sheets (Optional)**: When configured with `-s <SPREADSHEET_ID>`,
    the services read `GeneralConfig`, `ModelConfigs`, and `Materials` using
    read-only permissions (`spreadsheets.readonly`). No audit images or results
    are written back to Google Sheets by the pipeline.

### Data Retention & Deletion Controls

Because all pipeline storage lives in your GCP project, your organization
controls the retention and deletion lifecycle:

-   **Cloud Storage Lifecycle & Soft Delete**: You can configure
    [Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle)
    rules to automatically delete raw images and thumbnails after a defined
    retention period, and use
    [Cloud Storage Soft Delete](https://cloud.google.com/storage/docs/soft-delete)
    (for example, a 30-day soft-delete window) to protect against accidental
    deletion before permanent purge.
-   **BigQuery & Firestore Expiration**: You can delete individual rows or
    documents at any time, or configure
    [BigQuery table/partition expiration](https://cloud.google.com/bigquery/docs/managing-tables#updating_a_tables_expiration_time)
    and [Firestore TTL policies](https://firebase.google.com/docs/firestore/ttl)
    to enforce your organization's data retention schedule.

## Frequently Asked Questions (FAQ)

### 1. What is the estimated cost per image?

Total cost per audited image depends on the Gemini model you select, image
resolution and prompt/output token volume, the number of active prompt versions
run per image, and serverless infrastructure usage (Cloud Run, Cloud Storage,
Firestore, BigQuery, and Cloud Tasks). Because available model versions and
pricing evolve over time, check official Google Cloud and Gemini announcements,
the
[Vertex AI Pricing](https://cloud.google.com/vertex-ai/generative-ai/pricing)
page, and the
[Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator)
for the latest model options and cost estimates.

### 2. What open-source license governs this repository?

This project is released under the **Apache License 2.0**. See
[`LICENSE`](LICENSE) for the full license text.

### 3. How do we get support, report bugs, or contribute?

This repository is an open-source reference implementation and is not an
officially supported Google product. To report a bug, request a feature, or ask
a question about the code, please open an issue on GitHub. Contributions are
welcome—please review [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting a
pull request, and see [`SECURITY.md`](SECURITY.md) for reporting security
vulnerabilities.

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

