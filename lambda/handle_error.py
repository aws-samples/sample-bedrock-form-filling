"""
Lambda function to handle workflow errors.

This function:
1. Extracts job_id with fallback to "unknown"
2. Updates job status to "FAILED"
3. Stores error information in DynamoDB
4. Returns structured error response
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS client
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]


class ErrorHandlerError(Exception):
    """Custom exception for error handler failures."""

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


def extract_job_id(event: Dict[str, Any]) -> str:
    """
    Extract job_id from event with multiple fallback strategies.

    Args:
        event: Lambda event

    Returns:
        Job ID or "unknown" if not found
    """
    # Try direct access
    job_id = event.get("job_id")
    if job_id:
        return job_id

    # Try body field
    body = event.get("body")
    if body:
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                pass

        if isinstance(body, dict):
            job_id = body.get("job_id")
            if job_id:
                return job_id

    # Try error field
    error = event.get("error")
    if error and isinstance(error, dict):
        job_id = error.get("job_id")
        if job_id:
            return job_id

    # Try cause field (from Step Functions)
    cause = event.get("Cause")
    if cause:
        try:
            cause_data = json.loads(cause)
            job_id = cause_data.get("job_id")
            if job_id:
                return job_id
        except json.JSONDecodeError:
            pass

    log_event("WARNING", "Could not extract job_id from event")  # Event may contain sensitive data
    return "unknown"


def extract_error_info(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract error information from event.

    Args:
        event: Lambda event

    Returns:
        Dictionary containing error details
    """
    error_info: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Extract error message
    if "Error" in event:
        error_info["error_type"] = event.get("Error")

    if "error" in event:
        error_info["error_type"] = event.get("error")

    # Extract error message/cause
    if "Cause" in event:
        error_info["error_message"] = event.get("Cause")
    elif "message" in event:
        error_info["error_message"] = event.get("message")
    elif "errorMessage" in event:
        error_info["error_message"] = event.get("errorMessage")
    else:
        error_info["error_message"] = "Unknown error"

    # Extract stack trace if available
    if "errorType" in event:
        error_info["error_type"] = event.get("errorType")

    if "stackTrace" in event:
        error_info["stack_trace"] = event.get("stackTrace")

    # Determine if error is client (4xx) or server (5xx)
    status_code = event.get("statusCode")
    if status_code:
        error_info["status_code"] = status_code
        error_info["error_category"] = (
            "client_error" if 400 <= status_code < 500 else "server_error"
        )
    else:
        # Default to server error if not specified
        error_info["error_category"] = "server_error"
        error_info["status_code"] = 500

    return error_info


def update_job_failure(
    table_name: str, job_id: str, error_info: Dict[str, Any]
) -> None:
    """
    Update job status to FAILED with error information.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        error_info: Error information to store

    Raises:
        ErrorHandlerError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        # Check if job exists before updating
        try:
            response = table.get_item(Key={"job_id": job_id})
            job_exists = "Item" in response
        except ClientError:
            job_exists = False

        if not job_exists and job_id == "unknown":
            log_event(
                "WARNING",
                "Cannot update DynamoDB for unknown job_id",
                error_info=error_info,
            )
            return

        # Update job with error information
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, error_info = :error_info, failed_at = :failed_at, updated_at = :timestamp",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "FAILED",
                ":error_info": error_info,
                ":failed_at": timestamp,
                ":timestamp": timestamp,
            },
        )

        log_event(
            "INFO",
            "Job marked as failed",
            job_id=job_id,
            error_category=error_info.get("error_category"),
        )

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        log_event(
            "ERROR",
            "Failed to update job with error information",
            job_id=job_id,
        )
        # Don't raise exception - error handler should always succeed
        # to prevent cascading failures


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for error handling.

    Expected event format (varies based on error source):
    {
        "job_id": "uuid-string",
        "Error": "ErrorType",
        "Cause": "error details..."
    }

    Returns:
    {
        "statusCode": 500,
        "body": {
            "job_id": "uuid-string",
            "status": "FAILED",
            "error_info": {...},
            "message": "Job failed: error details"
        }
    }
    """
    log_event("ERROR", "Error handler Lambda invoked")  # Event structure varies, may contain sensitive error details

    try:
        # Extract job_id with fallback
        job_id = extract_job_id(event)

        # Extract error information
        error_info = extract_error_info(event)

        log_event(
            "INFO",
            "Handling workflow error",
            job_id=job_id,
            error_category=error_info.get("error_category"),
            error_type=error_info.get("error_type"),
        )

        # Update job status in DynamoDB (if job_id is known)
        if job_id != "unknown":
            update_job_failure(DYNAMODB_TABLE, job_id, error_info)

        # Build error response
        error_message = error_info.get("error_message", "Unknown error")
        response = {
            "statusCode": error_info.get("status_code", 500),
            "body": {
                "job_id": job_id,
                "status": "FAILED",
                "error_info": {
                    "error_type": error_info.get("error_type", "UnknownError"),
                    "error_message": error_message,
                    "error_category": error_info.get("error_category", "server_error"),
                    "timestamp": error_info["timestamp"],
                },
                "message": f"Job failed: {error_message}",
            },
        }

        log_event(
            "INFO",
            "Error handling completed",
            job_id=job_id,
            error_category=error_info.get("error_category"),
        )

        return response

    except Exception as e:
        # Error handler should never fail - return minimal error response
        log_event(
            "CRITICAL",
            "Error handler itself failed",
            error_type=type(e).__name__,
        )  # Avoid logging original_event - may contain sensitive error details

        return {
            "statusCode": 500,
            "body": {
                "job_id": "unknown",
                "status": "FAILED",
                "error_info": {
                    "error_type": "ErrorHandlerFailure",
                    "error_message": f"Error handler failed: {str(e)}",
                    "error_category": "server_error",
                    "timestamp": datetime.utcnow().isoformat(),
                },
                "message": "Critical error: error handler failed",
            },
        }
