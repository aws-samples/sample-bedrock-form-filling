# BDA Media Processing — Serverless Workflow

Transform media files into structured data using AWS Bedrock Data Automation and Bedrock Large Language Models.

![Frontend UI](assets/screenshot-frontend.png)


## Quick Start

### Prerequisites

1. **AWS Account** with Bedrock Data Automation available in your region
2. **AWS CLI** configured with admin credentials
3. **SAM CLI** installed ([installation guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html))

**Supported Regions:**
- `us-east-1` (US East - N. Virginia) - **default**
- `us-east-2` (US East - Ohio)
- `us-west-1` (US West - N. California)
- `us-west-2` (US West - Oregon)

**Required AWS Permissions:**
- CloudFormation (create/update/delete stacks)
- S3 (create buckets, upload objects)
- Lambda (create functions, manage permissions)
- API Gateway (create REST APIs)
- DynamoDB (create tables)
- Step Functions (create state machines)
- EventBridge (create rules)
- CloudFront (create distributions)
- Cognito (create user pools, manage users)
- IAM (create roles, attach policies)
- Bedrock (create data automation projects, invoke models)

### Deploy

Run the deployment script - it will prompt you for minimal configuration:

```bash
./deploy.sh
```

**You'll be asked for only 2 things:**

1. **Region** (optional)
   - Press Enter to use default: `us-east-1`
   - Or specify: `us-east-2`, `us-west-1`, `us-west-2`

2. **Stack Name** (optional)
   - Press Enter to use default: `bda-media-processing`
   - Or provide custom name

**The script automatically:**
- Builds and deploys all infrastructure (CloudFormation stack)
- Creates BDA project with comprehensive standard outputs for all 4 modalities
- Configures API Gateway + Lambda functions + Step Functions
- Sets up CloudFront distribution + S3 buckets
- Creates Cognito user pool with test credentials
- Deploys frontend application
- Displays your application URL

**Deployment time:** ~5-7 minutes

### Access

1. Open the **CloudFront URL** shown at the end of deployment
2. Login with:
   - **Username:** `test@example.com`
   - **Password:** `Test123!`
3. Upload media files and process

**Note:** CloudFront may take 5-15 minutes to fully deploy on first run.

## Architecture

![AWS Architecture](assets/architecture.png)

**Main Workflow (Steps 1-14):**

1. **User** uploads media via frontend
2. **S3 Raw Media** receives file (triggers EventBridge)
3. **EventBridge** starts Step Functions workflow
4. **Lambda Initialize Job** creates DynamoDB record
5. **Lambda BDA Trigger** starts Bedrock Data Automation with task token
6. **Amazon BDA** processes media asynchronously (transcription, OCR)
7. **EventBridge** receives completion event, resumes workflow
8. **S3 BDA Results** stores extracted content
9. **Lambda Extract Results** retrieves content from BDA
10. **S3 Processed Media** stores processed results
11. **Lambda Get Structured Data** sends content to LLM
12. **Bedrock LLM** converts to structured JSON
13. **Lambda Results Validator** validates against schema
14. **S3 Final Results** stores completed data

**Status Polling (Independent):**
- **Lambda Get Status** queries DynamoDB on-demand (HTTP GET every 5s)
- Completely decoupled from Step Functions workflow
- Reads job status written by workflow Lambdas

Step Functions uses `.waitForTaskToken` with EventBridge — execution pauses at step 5, BDA runs async, EventBridge automatically resumes at step 7. No polling loops needed.

**Components:**
- **Frontend:** CloudFront + S3 + Cognito authentication
- **APIs:** API Gateway (2 endpoints: create job, get status) with Cognito authorization
- **Orchestration:** Step Functions + EventBridge
- **Processing:** Bedrock Data Automation + Bedrock LLM
- **Storage:** DynamoDB (job tracking with user isolation) + S3 (media & results)
- **Compute:** 7 Lambda functions (6 workflow + 2 API endpoints)

## Custom Forms

Define exactly what data to extract from media content:

```json
{
  "form_id": "meeting_analysis_v1",
  "sections": [
    {
      "section_id": "content",
      "fields": [
        {
          "field_id": "meeting_type",
          "field_type": "select",
          "options": ["standup", "planning", "retrospective"]
        },
        {
          "field_id": "key_decisions",
          "field_type": "text"
        }
      ]
    }
  ]
}
```

**Field Types:**
- `text` — Free-form extraction
- `select` — Multiple choice
- `radio` — Single choice

## API Reference

**Authentication:** Both endpoints require Cognito authentication via JWT token in `Authorization` header.

### Create Job
```bash
POST /jobs
Headers:
  Authorization: Bearer {cognito_id_token}

{
  "filename": "meeting.mp3",
  "form_schema": { ... }  # Optional
}

Response:
{
  "job_id": "uuid",
  "upload_url": "https://...",
  "status": "CREATED"
}
```

Jobs are automatically associated with the authenticated user (`user_id` from Cognito `sub` claim).

### Get Status
```bash
GET /jobs/{job_id}
Headers:
  Authorization: Bearer {cognito_id_token}

Response:
{
  "job_id": "uuid",
  "status": "COMPLETED",
  "transcript_key": "transcripts/{job_id}/transcript.txt",
  "structured_data_key": "results/{job_id}/structured-data.json"
}
```

**Access Control:** Users can only access their own jobs. Attempting to access another user's job returns `403 Forbidden`.

**Status Flow:**
`CREATED` → `INITIALIZING` → `BDA_PROCESSING` → `EXTRACTING_RESULTS` → `PROCESSING_STRUCTURED_DATA` → `COMPLETED`

## Project Structure

```
├── assets/                      # Sample files
├── frontend/                    # Web UI
│   └── index.html              # Single-file interface
├── infrastructure/              # AWS SAM
│   └── template.yaml           # Complete infrastructure
└── lambda/                      # Python functions
    ├── api_create_job.py       # POST /jobs (with user auth)
    ├── api_get_status.py       # GET /jobs/{id} (with access control)
    ├── auth_utils.py           # Cognito JWT claim extraction
    ├── initialize_job.py       # Start workflow
    ├── bda_trigger.py          # Trigger BDA
    ├── bda_eventbridge_handler.py  # Handle BDA completion events
    ├── extract_results.py      # Extract content
    ├── structured_data.py      # LLM extraction
    ├── validate_results.py     # Validate structured data
    ├── complete_job.py         # Finish job
    └── handle_error.py         # Error handling
```

## Configuration

**BDA Profile:**
The deployment automatically uses the default cross-region BDA profile:
```
arn:aws:bedrock:{region}:{account}:data-automation-profile/us.data-automation-v1
```
This profile provides cross-region inference across all 4 US regions (us-east-1, us-east-2, us-west-1, us-west-2).

**BDA Project:**
A BDA project is automatically created by CloudFormation with all standard outputs enabled for:
- **Documents**: Full text extraction (DOCUMENT, PAGE, ELEMENT, WORD, LINE granularity), bounding boxes, generative fields, multiple output formats (MARKDOWN, PLAIN_TEXT, additional file formats)
- **Images**: Text detection, content moderation, logo detection, bounding boxes, image summaries, IAB classification
- **Video**: Transcripts, text detection, content moderation, logo detection, bounding boxes, video summaries, chapter summaries, IAB classification
- **Audio**: Transcripts, audio content moderation, topic content moderation, audio summaries, topic summaries, IAB classification

**Bedrock LLM Model:**
Structured data extraction uses `us.amazon.nova-lite-v1:0` (configurable in `infrastructure/template.yaml`)

**IAB Classification:**
IAB (Interactive Advertising Bureau) provides standardized content classification for digital advertising and content categorization. Enabled for image, video, and audio processing.

## Clean Up

Delete the CloudFormation stack to remove all resources:

```bash
aws cloudformation delete-stack --stack-name bda-media-processing --region us-east-1
```

Replace `bda-media-processing` with your stack name if you used a different one.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

