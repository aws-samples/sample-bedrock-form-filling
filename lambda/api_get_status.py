"""
API Lambda function to get job status.

This function is DECOUPLED from the main workflow and serves as the status
endpoint for the frontend. It queries DynamoDB for the current job status
and returns it to the caller.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from auth_utils import get_user_id_from_event

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
S3_BUCKET = os.environ.get("S3_BUCKET")

if not DYNAMODB_TABLE:
    raise ValueError("DYNAMODB_TABLE environment variable is required")

# AWS clients
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")


def get_table():
    """Get DynamoDB table reference."""
    return dynamodb.Table(DYNAMODB_TABLE)


def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve job status from DynamoDB.

    Args:
        job_id: Unique identifier for the job

    Returns:
        Job record dictionary or None if not found

    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        table = get_table()
        response = table.get_item(Key={"job_id": job_id})

        if "Item" not in response:
            logger.warning(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "WARNING",
                        "message": "Job not found",
                        "job_id": job_id,
                    }
                )
            )
            return None

        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Retrieved job status",
                    "job_id": job_id,
                    "status": response["Item"].get("status", "UNKNOWN"),
                }
            )
        )

        return response["Item"]

    except ClientError as e:
        logger.error(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "ERROR",
                    "message": "Failed to retrieve job status",
                    "error": str(e),
                    "job_id": job_id,
                }
            )
        )
        raise


def get_structured_data(s3_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch structured data from S3.

    Args:
        s3_key: S3 key for the structured data JSON file

    Returns:
        Structured data dictionary or None if not found/error
    """
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.info(f"Structured data not found at {s3_key}")
        else:
            logger.error(f"Error fetching structured data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching structured data: {e}")
        return None


def cors_headers() -> Dict[str, Any]:
    """Return CORS headers for API response."""
    return {
        "Access-Control-Allow-Origin": os.environ["ALLOWED_ORIGIN"],
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for job status API.

    This function retrieves the current status of a job from DynamoDB.

    Args:
        event: API Gateway event with structure:
            {
                "pathParameters": {"job_id": "uuid"},
                "queryStringParameters": {"job_id": "uuid"}  # Alternative
            }
        context: Lambda context object

    Returns:
        API Gateway response:
            {
                "statusCode": 200,
                "headers": {...},
                "body": "{
                    \"job_id\": \"uuid\",
                    \"status\": \"PROCESSING_STRUCTURED_DATA\",
                    \"created_at\": \"2024-01-01T00:00:00\",
                    \"updated_at\": \"2024-01-01T00:05:00\",
                    ...
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
                    "message": "Received status API request",
                    "path": event.get("path"),
                    "method": event.get("httpMethod"),
                }
            )
        )

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

        # Extract job_id from path parameters or query string
        job_id = None

        if "pathParameters" in event and event["pathParameters"]:
            job_id = event["pathParameters"].get("job_id")

        if not job_id and "queryStringParameters" in event and event["queryStringParameters"]:
            job_id = event["queryStringParameters"].get("job_id")

        if not job_id:
            logger.error(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "ERROR",
                        "message": "Missing job_id parameter",
                    }
                )
            )
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"error": "Missing job_id parameter"}),
            }

        # Get job status from DynamoDB
        job_record = get_job_status(job_id)

        if not job_record:
            return {
                "statusCode": 404,
                "headers": cors_headers(),
                "body": json.dumps(
                    {"error": "Job not found", "job_id": job_id}
                ),
            }

        # Validate user_id ownership to prevent path traversal attacks
        job_owner_id = job_record.get("user_id")
        if job_owner_id and job_owner_id != user_id:
            logger.warning(
                json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "WARNING",
                        "message": "User attempted to access job owned by another user",
                        "job_id": job_id,
                        "requesting_user_id": user_id,
                        "job_owner_id": job_owner_id,
                    }
                )
            )
            return {
                "statusCode": 403,
                "headers": cors_headers(),
                "body": json.dumps(
                    {"error": "Forbidden: You do not have permission to access this job"}
                ),
            }

        # If job is completed, include structured data
        if job_record.get("status") == "COMPLETED" and job_record.get("structured_data_key"):
            structured_data = get_structured_data(job_record["structured_data_key"])
            if structured_data:
                job_record["structured_data"] = structured_data

        # Return job record
        logger.info(
            json.dumps(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Status request successful",
                    "job_id": job_id,
                    "status": job_record.get("status", "UNKNOWN"),
                }
            )
        )

        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps(job_record, default=str),
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
