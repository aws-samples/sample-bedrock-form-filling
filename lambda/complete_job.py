"""
Lambda function to complete media processing job.

This function:
1. Updates job status to "COMPLETED"
2. Sets completed_at timestamp
3. Returns success message with job summary
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS client
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]


class CompletionError(Exception):
    """Custom exception for job completion errors."""

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


def get_job_details(table_name: str, job_id: str) -> Dict[str, Any]:
    """
    Retrieve job details from DynamoDB.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier

    Returns:
        Job record from DynamoDB

    Raises:
        CompletionError: If retrieval fails
    """
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={"job_id": job_id})

        if "Item" not in response:
            raise CompletionError(f"Job not found: {job_id}")

        return response["Item"]

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve job details",
            job_id=job_id,
        )
        raise CompletionError(f"Failed to retrieve job: {e}") from e


def update_job_completion(table_name: str, job_id: str) -> None:
    """
    Update job status to COMPLETED with completion timestamp.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier

    Raises:
        CompletionError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, completed_at = :completed_at, updated_at = :timestamp",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "COMPLETED",
                ":completed_at": timestamp,
                ":timestamp": timestamp,
            },
        )
        log_event(
            "INFO",
            "Job marked as completed",
            job_id=job_id,
            completed_at=timestamp,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update job completion",
            job_id=job_id,
        )
        raise CompletionError(f"DynamoDB update failed: {e}") from e


def calculate_processing_time(job: Dict[str, Any]) -> float:
    """
    Calculate total processing time in seconds.

    Args:
        job: Job record from DynamoDB

    Returns:
        Processing time in seconds (0.0 if cannot calculate)
    """
    try:
        created_at = job.get("created_at")
        if not created_at:
            return 0.0

        created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        completed_time = datetime.utcnow()

        delta = completed_time - created_time
        return delta.total_seconds()

    except (ValueError, TypeError) as e:
        log_event(
            "WARNING",
            "Failed to calculate processing time",
            job_id=job.get("job_id"),
        )
        return 0.0


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for completing audio processing job.

    Expected event format:
    {
        "job_id": "uuid-string",
        "is_valid": true
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "status": "COMPLETED",
            "message": "Job completed successfully",
            "summary": {
                "filename": "audio.mp3",
                "transcript_key": "transcripts/...",
                "structured_data_key": "results/...",
                "is_valid": true,
                "processing_time_seconds": 45.2
            }
        }
    }
    """
    log_event("INFO", "Complete job Lambda invoked")

    try:
        # Extract and validate input
        job_id = event.get("job_id")
        is_valid = event.get("is_valid", False)

        if not job_id:
            raise CompletionError("Missing required field: job_id")

        log_event("INFO", "Completing job", job_id=job_id)

        # Retrieve job details
        job = get_job_details(DYNAMODB_TABLE, job_id)

        # Update job status to COMPLETED
        update_job_completion(DYNAMODB_TABLE, job_id)

        # Calculate processing time
        processing_time = calculate_processing_time(job)

        # Build job summary
        summary = {
            "filename": job.get("filename"),
            "transcript_key": job.get("transcript_key"),
            "structured_data_key": job.get("structured_data_key"),
            "is_valid": is_valid,
            "processing_time_seconds": round(processing_time, 2),
        }

        # Return success response
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "status": "COMPLETED",
                "message": "Job completed successfully",
                "summary": summary,
            },
        }

        log_event(
            "INFO",
            "Job completion successful",
            job_id=job_id,
            processing_time_seconds=processing_time,
        )

        return response

    except CompletionError as e:
        log_event("ERROR", "Completion error")
        return {
            "statusCode": 400,
            "error": "CompletionError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error during job completion")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred during job completion",
        }
