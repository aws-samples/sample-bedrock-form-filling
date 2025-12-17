"""
Lambda function to initialize media processing job.

This function:
1. Generates a unique job_id (UUID)
2. Creates a DynamoDB record with status="INITIALIZING"
3. Copies media file from raw-media to processed-media
4. Updates status to "PREPROCESSED"
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw-media")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed-media")


class InitializationError(Exception):
    """Custom exception for initialization errors."""

    pass


def log_event(level: str, message: str, **kwargs) -> None:
    """Log structured JSON message."""
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        **kwargs,
    }
    logger.info(json.dumps(log_data))


def generate_job_id() -> str:
    """Generate a unique job ID using UUID4."""
    return str(uuid.uuid4())


def create_dynamodb_record(
    table_name: str, job_id: str, filename: str, status: str, raw_key: str = None, processed_key: str = None
) -> None:
    """
    Create initial DynamoDB record for the job.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Unique job identifier
        filename: Original audio filename
        status: Initial job status
        raw_key: S3 key of raw uploaded file (optional)
        processed_key: S3 key of processed file (optional)

    Raises:
        InitializationError: If DynamoDB write fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        # Build update expression dynamically
        update_expr = "SET #status = :status, filename = :filename, updated_at = :updated_at"
        expr_values = {
            ":status": status,
            ":filename": filename,
            ":updated_at": timestamp,
        }

        if raw_key:
            update_expr += ", raw_key = :raw_key"
            expr_values[":raw_key"] = raw_key

        if processed_key:
            update_expr += ", processed_key = :processed_key"
            expr_values[":processed_key"] = processed_key

        # Use update_item instead of put_item to preserve existing attributes
        # (form_schema, form_id, definitions, pre_filled_values, etc.)
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={
                "#status": "status"  # 'status' is a reserved word in DynamoDB
            },
            ExpressionAttributeValues=expr_values,
        )
        log_event(
            "INFO",
            "DynamoDB record updated",
            job_id=job_id,
            status=status,
            filename=filename,
            raw_key=raw_key,
            processed_key=processed_key,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to create DynamoDB record",
            job_id=job_id,
        )
        raise InitializationError(f"DynamoDB write failed: {e}") from e


def copy_media_file(
    bucket: str, source_key: str, destination_key: str, job_id: str
) -> None:
    """
    Copy media file from raw to processed location in S3.

    Args:
        bucket: S3 bucket name
        source_key: Source S3 key
        destination_key: Destination S3 key
        job_id: Job identifier for logging

    Raises:
        InitializationError: If S3 copy fails
    """
    copy_source = {"Bucket": bucket, "Key": source_key}

    try:
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=destination_key)
        log_event(
            "INFO",
            "Media file copied",
            job_id=job_id,
            source_key=source_key,
            destination_key=destination_key,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to copy media file",
            job_id=job_id,
            source_key=source_key,
            destination_key=destination_key,
        )
        raise InitializationError(f"S3 copy failed: {e}") from e


def update_job_status(table_name: str, job_id: str, status: str) -> None:
    """
    Update job status in DynamoDB.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        status: New status value

    Raises:
        InitializationError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, updated_at = :timestamp",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": status, ":timestamp": timestamp},
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update job status",
            job_id=job_id,
            status=status,
        )
        raise InitializationError(f"DynamoDB update failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for initializing media processing job.

    Expected event format (from EventBridge S3 event):
    {
        "bucket": "bucket-name",
        "key": "raw-media/job-id/filename.ext"
    }

    OR (from API):
    {
        "filename": "media.ext"
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "processed_key": "processed-media/job-id/media.ext",
            "status": "PREPROCESSED"
        }
    }
    """
    log_event("INFO", "Initialize job Lambda invoked")

    try:
        # Handle both EventBridge (bucket/key) and direct invocation (filename)
        if "key" in event and "bucket" in event:
            # EventBridge S3 event format
            s3_key = event["key"]
            # Extract job_id and filename from key: raw-audio/job-id/filename.mp3
            parts = s3_key.split("/")
            if len(parts) < 3:
                raise InitializationError(f"Invalid S3 key format: {s3_key}")
            job_id = parts[1]
            filename = parts[2]
            log_event("INFO", "Extracted from S3 key", job_id=job_id)
        else:
            # Direct invocation format
            filename = event.get("filename")
            if not filename:
                raise InitializationError("Missing required field: filename or key")
            # Generate unique job ID
            job_id = generate_job_id()
            log_event("INFO", "Generated job ID", job_id=job_id)

        # Construct S3 keys
        raw_key = f"{RAW_PREFIX}/{job_id}/{filename}"
        processed_key = f"{PROCESSED_PREFIX}/{job_id}/{filename}"

        # Create initial DynamoDB record with S3 keys
        create_dynamodb_record(DYNAMODB_TABLE, job_id, filename, "INITIALIZING", raw_key, processed_key)

        # Copy media file to processed location
        copy_media_file(S3_BUCKET, raw_key, processed_key, job_id)

        # Update status to PREPROCESSED
        update_job_status(DYNAMODB_TABLE, job_id, "PREPROCESSED")

        # Return success response
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "processed_key": processed_key,
                "status": "PREPROCESSED",
            },
        }

        log_event(
            "INFO",
            "Job initialization completed successfully",
            job_id=job_id,
            processed_key=processed_key,
        )

        return response

    except InitializationError as e:
        log_event("ERROR", "Initialization error")
        return {
            "statusCode": 400,
            "error": "InitializationError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error during initialization")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred during job initialization",
        }
