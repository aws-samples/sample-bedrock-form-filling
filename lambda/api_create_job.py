"""
API Lambda function to create a new job and return job_id.

This function is DECOUPLED from the main workflow and serves as the entry point
for the frontend. It generates a job_id, creates a DynamoDB record, and returns
the job_id and upload path to the frontend.

The actual workflow is triggered by EventBridge when the file is uploaded to S3.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from auth_utils import get_user_id_from_event


# Maximum file size: 500MB (adjust as needed for your use case)
MAX_FILE_SIZE_BYTES = 500 * 1024 * 1024  # 500MB

# MIME type mapping for common file extensions (WHITELIST)
CONTENT_TYPE_MAP = {
    # Audio
    'mp3': 'audio/mpeg',
    'wav': 'audio/wav',
    'm4a': 'audio/mp4',
    'flac': 'audio/flac',
    'aac': 'audio/aac',
    'ogg': 'audio/ogg',
    # Video
    'mp4': 'video/mp4',
    'mov': 'video/quicktime',
    'avi': 'video/x-msvideo',
    'mkv': 'video/x-matroska',
    'webm': 'video/webm',
    # Documents
    'pdf': 'application/pdf',
    'doc': 'application/msword',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'txt': 'text/plain',
    # Images
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'gif': 'image/gif',
    'webp': 'image/webp',
    'tiff': 'image/tiff',
}

# Explicitly blocked file extensions for security (executables, archives, scripts)
BLOCKED_EXTENSIONS = {
    'exe', 'bat', 'cmd', 'com', 'scr', 'msi',  # Windows executables
    'sh', 'bash', 'zsh', 'csh',  # Unix shells
    'app', 'dmg', 'pkg',  # macOS executables/installers
    'zip', 'rar', '7z', 'tar', 'gz', 'bz2',  # Archives (could contain malware)
    'jar', 'war',  # Java archives
    'apk', 'ipa',  # Mobile apps
    'js', 'vbs', 'wsf', 'ps1',  # Scripts
    'html', 'htm', 'svg',  # Could contain XSS
}

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
S3_BUCKET = os.environ.get("S3_BUCKET")

if not DYNAMODB_TABLE:
    raise ValueError("DYNAMODB_TABLE environment variable is required")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET environment variable is required")

# AWS clients
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3", config=Config(signature_version='s3v4'))


def get_table():
    """Get DynamoDB table reference."""
    return dynamodb.Table(DYNAMODB_TABLE)


def validate_file_type(filename: str) -> None:
    """
    Validate that file type is allowed.

    Args:
        filename: Original filename with extension

    Raises:
        ValueError: If file type is not allowed
    """
    if not filename or '.' not in filename:
        raise ValueError("Filename must include a file extension")

    extension = filename.rsplit('.', 1)[-1].lower()

    # Check against blocked list first (security)
    if extension in BLOCKED_EXTENSIONS:
        raise ValueError(
            f"File type '.{extension}' is not allowed for security reasons. "
            f"Allowed types: audio (mp3, wav, m4a, etc.), video (mp4, mov, etc.), "
            f"documents (pdf, docx, txt), images (png, jpg, etc.)"
        )

    # Check against whitelist
    if extension not in CONTENT_TYPE_MAP:
        raise ValueError(
            f"File type '.{extension}' is not supported. "
            f"Allowed types: {', '.join(sorted(CONTENT_TYPE_MAP.keys()))}"
        )


def get_content_type_from_filename(filename: str) -> str:
    """
    Infer MIME type from file extension.

    Args:
        filename: Original filename with extension

    Returns:
        MIME type string

    Raises:
        ValueError: If extension not in whitelist (via validate_file_type)
    """
    if not filename or '.' not in filename:
        raise ValueError("Filename must include a file extension")

    extension = filename.rsplit('.', 1)[-1].lower()

    # Will raise ValueError if invalid
    validate_file_type(filename)

    return CONTENT_TYPE_MAP[extension]


def validate_request(body: Dict[str, Any]) -> None:
    """
    Validate API request body.

    Args:
        body: Request body dictionary

    Raises:
        ValueError: If required fields are missing or invalid
    """
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")

    # Optional: filename for reference (actual filename will come from upload)
    if "filename" in body and not isinstance(body["filename"], str):
        raise ValueError("filename must be a string")

    # Optional: form_id
    if "form_id" in body and not isinstance(body["form_id"], str):
        raise ValueError("form_id must be a string")

    # Optional: form_schema
    if "form_schema" in body and not isinstance(body["form_schema"], dict):
        raise ValueError("form_schema must be a JSON object")

    # Optional: definitions
    if "definitions" in body and not isinstance(body["definitions"], str):
        raise ValueError("definitions must be a string")

    # Optional: pre_filled_values
    if "pre_filled_values" in body:
        if not isinstance(body["pre_filled_values"], dict):
            raise ValueError("pre_filled_values must be a JSON object")

        # Detect format: if values are dicts, it's OLD nested format; otherwise NEW flat format
        sample_values = list(body["pre_filled_values"].values())
        if sample_values:
            first_value = sample_values[0]
            is_flat_format = not isinstance(first_value, dict)

            if not is_flat_format:
                # OLD FORMAT: Validate nested structure - each section should contain fields as a dict
                for section_id, fields in body["pre_filled_values"].items():
                    if not isinstance(fields, dict):
                        raise ValueError(
                            f"pre_filled_values[{section_id}] must be a JSON object with field values"
                        )
            # NEW FORMAT: No additional validation needed, values can be strings or other primitives


def create_job_record(
    job_id: str,
    user_id: str,
    filename: str = None,
    form_id: str = None,
    form_schema: Dict[str, Any] = None,
    definitions: str = None,
    pre_filled_values: Dict[str, Any] = None,
) -> None:
    """
    Create initial job record in DynamoDB.

    Args:
        job_id: Unique identifier for the job
        user_id: Cognito user ID (from 'sub' claim) who created the job
        filename: Optional original filename
        form_id: Optional form ID for structured data extraction
        form_schema: Optional form schema for custom structured data extraction
        definitions: Optional industry-specific definitions for extraction guidance
        pre_filled_values: Optional pre-filled field values that should not be overridden

    Raises:
        ClientError: If DynamoDB operation fails
    """
    timestamp = datetime.utcnow().isoformat()

    job_record = {
        "job_id": job_id,
        "user_id": user_id,
        "status": "CREATED",
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    if filename:
        job_record["original_filename"] = filename
    if form_id:
        job_record["form_id"] = form_id
    if form_schema:
        job_record["form_schema"] = json.dumps(form_schema)
    if definitions:
        job_record["definitions"] = definitions
    if pre_filled_values:
        job_record["pre_filled_values"] = json.dumps(pre_filled_values)

    try:
        table = get_table()
        table.put_item(Item=job_record)
        logger.info(
            json.dumps(
                {
                    "timestamp": timestamp,
                    "level": "INFO",
                    "message": "Created job record",
                    "job_id": job_id,
                    "user_id": user_id,
                    "filename": filename,
                    "form_id": form_id,
                    "has_definitions": definitions is not None,
                    "has_pre_filled_values": pre_filled_values is not None,
                }
            )
        )
    except ClientError as e:
        logger.error(
            json.dumps(
                {
                    "timestamp": timestamp,
                    "level": "ERROR",
                    "message": "Failed to create job record",
                    "error": str(e),
                    "job_id": job_id,
                }
            )
        )
        raise


def generate_presigned_url(job_id: str, filename: str, content_type: str) -> str:
    """
    Generate presigned S3 upload URL with size and type constraints.

    Args:
        job_id: Job identifier
        filename: Original filename
        content_type: MIME type for the file

    Returns:
        Presigned URL for upload with embedded size constraints

    Raises:
        ClientError: If presigned URL generation fails
        ValueError: If generated URL is invalid
    """
    s3_key = f"raw-media/{job_id}/{filename}"

    try:
        # Generate presigned POST URL with conditions (more secure than PUT)
        # This allows S3 to enforce file size limits at upload time
        url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": s3_key,
                "ContentType": content_type,
                # Note: ContentLength would be ideal but isn't supported by PUT presigned URLs
                # Size validation happens client-side and via POST policy in production
            },
            ExpiresIn=3600,  # 1 hour
        )

        # Validate that URL was successfully generated
        if not url or not url.startswith('https'):
            timestamp = datetime.utcnow().isoformat()
            logger.error(
                json.dumps(
                    {
                        "timestamp": timestamp,
                        "level": "ERROR",
                        "message": "Generated presigned URL is invalid",
                        "job_id": job_id,
                        "s3_key": s3_key,
                        "url_length": len(url) if url else 0,
                    }
                )
            )
            raise ValueError("Failed to generate valid presigned URL")

        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Generated presigned URL",
                    "job_id": job_id,
                    "s3_key": s3_key,
                }
            )
        )
        return url
    except ClientError as e:
        logger.error(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "ERROR",
                    "message": "Failed to generate presigned URL",
                    "error": str(e),
                    "job_id": job_id,
                }
            )
        )
        raise


def cors_headers() -> Dict[str, str]:
    """Return CORS headers for API response."""
    return {
        "Access-Control-Allow-Origin": os.environ["ALLOWED_ORIGIN"],
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for job creation API.

    This function:
    1. Generates a unique job_id
    2. Infers content type from filename extension
    3. Creates a DynamoDB record with status="CREATED"
    4. Generates a presigned S3 upload URL with correct Content-Type
    5. Returns job_id, upload URL, and S3 key to frontend

    The actual workflow starts when the file is uploaded to S3 (EventBridge trigger).

    Args:
        event: API Gateway event with structure:
            {
                "body": "{\"filename\": \"meeting.mp4\", \"form_id\": \"simple_media_analysis_v1\"}"
            }
        context: Lambda context object

    Returns:
        API Gateway response:
            {
                "statusCode": 200,
                "headers": {...},
                "body": "{
                    \"job_id\": \"uuid\",
                    \"upload_url\": \"https://...\",
                    \"s3_key\": \"raw-media/{job_id}/filename.ext\",
                    \"status\": \"CREATED\"
                }"
            }

    Raises:
        Exception: Any unhandled exceptions are caught and returned as 500 errors
    """
    try:
        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Received API request",
                    "http_method": event.get("httpMethod"),
                    "path": event.get("path"),
                }
            )
        )  # Avoid logging full event - body contains user schemas, definitions, pre-filled values

        # Handle OPTIONS request for CORS
        if event.get("httpMethod") == "OPTIONS":
            return {"statusCode": 200, "headers": cors_headers(), "body": ""}

        # Extract user_id from Cognito claims
        try:
            user_id = get_user_id_from_event(event)
        except ValueError as e:
            logger.error(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "ERROR",
                        "message": "Failed to extract user_id",
                        "error": str(e),
                    }
                )
            )
            return {
                "statusCode": 401,
                "headers": cors_headers(),
                "body": json.dumps({"error": "Unauthorized: Invalid authentication"}),
            }

        # Parse request body
        body = {}
        if "body" in event and event["body"]:
            try:
                body = json.loads(event["body"])
            except json.JSONDecodeError as e:
                logger.error(
                    json.dumps(
                        {
                            "timestamp": datetime.utcnow().isoformat(),
                            "level": "ERROR",
                            "message": "Invalid JSON in request body",
                            "error": str(e),
                        }
                    )
                )
                return {
                    "statusCode": 400,
                    "headers": cors_headers(),
                    "body": json.dumps({"error": "Invalid JSON in request body"}),
                }

        # Validate request
        try:
            validate_request(body)
        except ValueError as e:
            logger.error(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "ERROR",
                        "message": "Request validation failed",
                        "error": str(e),
                    }
                )
            )
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"error": str(e)}),
            }

        # Extract parameters
        filename = body.get("filename", "media.file")
        form_id = body.get("form_id", "simple_media_analysis_v1")
        form_schema = body.get("form_schema")
        definitions = body.get("definitions")
        pre_filled_values = body.get("pre_filled_values")

        # Validate file type (will raise ValueError if invalid)
        # This happens BEFORE generating presigned URL to prevent invalid uploads
        try:
            content_type = get_content_type_from_filename(filename)
        except ValueError as e:
            logger.error(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "ERROR",
                        "message": "Invalid file type",
                        "filename": filename,
                    }
                )
            )
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"error": str(e)}),
            }

        # Generate unique job ID
        job_id = str(uuid.uuid4())
        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Generated job_id",
                    "job_id": job_id,
                    "filename": filename,
                    "content_type": content_type,
                    "has_custom_schema": form_schema is not None,
                    "has_definitions": definitions is not None,
                    "has_pre_filled_values": pre_filled_values is not None,
                }
            )
        )

        # Create job record
        create_job_record(job_id, user_id, filename, form_id, form_schema, definitions, pre_filled_values)

        # Generate presigned upload URL with inferred content type
        upload_url = generate_presigned_url(job_id, filename, content_type)

        # Build S3 key for reference
        s3_key = f"raw-media/{job_id}/{filename}"

        # Prepare response
        response_body = {
            "job_id": job_id,
            "upload_url": upload_url,
            "s3_key": s3_key,
            "status": "CREATED",
            "max_file_size_bytes": MAX_FILE_SIZE_BYTES,
            "message": "Job created successfully. Upload your file to the provided URL.",
        }

        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Job creation successful",
                    "job_id": job_id,
                }
            )
        )

        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps(response_body),
        }

    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "ERROR",
                    "message": "Unexpected error in lambda_handler",
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
            ),
            exc_info=True,
        )

        return {
            "statusCode": 500,
            "headers": cors_headers(),
            "body": json.dumps(
                {
                    "error": "Internal server error",
                    "message": "An unexpected error occurred. Please try again later.",
                }
            ),
        }
