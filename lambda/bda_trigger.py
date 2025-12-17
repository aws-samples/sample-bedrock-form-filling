"""
Lambda function to trigger Bedrock Data Automation job.

This function:
1. Invokes Bedrock Data Automation API asynchronously for any media type
2. Stores bda_invocation_id in DynamoDB
3. Updates job status to "BDA_PROCESSING"
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

# Initialize AWS clients
bedrock_client = boto3.client("bedrock-data-automation-runtime")
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
BDA_PROFILE_ARN = os.environ["BDA_PROFILE_ARN"]
BDA_PROJECT_ARN = os.environ["BDA_PROJECT_ARN"]
BDA_OUTPUT_PREFIX = os.environ.get("BDA_OUTPUT_PREFIX", "bda-output")


class BDATriggerError(Exception):
    """Custom exception for BDA trigger errors."""

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


def invoke_bda_job(
    profile_arn: str,
    project_arn: str,
    input_s3_uri: str,
    output_s3_uri: str,
    job_id: str,
) -> str:
    """
    Invoke Bedrock Data Automation job asynchronously with EventBridge enabled.

    Args:
        profile_arn: ARN of the BDA profile
        project_arn: ARN of the BDA project
        input_s3_uri: S3 URI of input media file (audio, video, document, image)
        output_s3_uri: S3 URI for output location
        job_id: Job identifier for logging

    Returns:
        BDA invocation ID

    Raises:
        BDATriggerError: If BDA invocation fails
    """
    try:
        response = bedrock_client.invoke_data_automation_async(
            dataAutomationProfileArn=profile_arn,
            inputConfiguration={
                "s3Uri": input_s3_uri,
            },
            outputConfiguration={
                "s3Uri": output_s3_uri,
            },
            dataAutomationConfiguration={
                "dataAutomationProjectArn": project_arn,
            },
            notificationConfiguration={
                "eventBridgeConfiguration": {
                    "eventBridgeEnabled": True
                }
            },
        )

        invocation_id = response.get("invocationArn")
        if not invocation_id:
            raise BDATriggerError("BDA response missing invocationArn")

        log_event(
            "INFO",
            "BDA job invoked successfully",
            job_id=job_id,
            invocation_id=invocation_id,
            input_uri=input_s3_uri,
            output_uri=output_s3_uri,
        )

        return invocation_id

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to invoke BDA job",
            job_id=job_id,
        )
        raise BDATriggerError(f"BDA invocation failed: {e}") from e


def update_job_with_bda_id(
    table_name: str, job_id: str, bda_invocation_id: str, status: str, task_token: str = None
) -> None:
    """
    Update DynamoDB with BDA invocation ID, status, and task token.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        bda_invocation_id: BDA invocation ID
        status: New job status
        task_token: Step Functions task token for callback (optional)

    Raises:
        BDATriggerError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        # Build update expression conditionally based on task_token
        if task_token:
            update_expression = "SET bda_invocation_id = :bda_id, #status = :status, updated_at = :timestamp, task_token = :token"
            expression_values = {
                ":bda_id": bda_invocation_id,
                ":status": status,
                ":timestamp": timestamp,
                ":token": task_token,
            }
        else:
            update_expression = "SET bda_invocation_id = :bda_id, #status = :status, updated_at = :timestamp"
            expression_values = {
                ":bda_id": bda_invocation_id,
                ":status": status,
                ":timestamp": timestamp,
            }

        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues=expression_values,
        )
        log_event(
            "INFO",
            "DynamoDB updated with BDA invocation ID",
            job_id=job_id,
            bda_invocation_id=bda_invocation_id,
            status=status,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update DynamoDB",
            job_id=job_id,
        )
        raise BDATriggerError(f"DynamoDB update failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for triggering BDA job with EventBridge callback.

    Expected event format:
    {
        "job_id": "uuid-string",
        "processed_key": "processed-media/job-id/media.ext",
        "task_token": "step-functions-task-token"  // Optional, for callback pattern
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "bda_invocation_id": "arn:aws:bedrock:...",
            "status": "BDA_PROCESSING"
        }
    }
    """
    log_event("INFO", "BDA trigger Lambda invoked")

    try:
        # Extract and validate input
        job_id = event.get("job_id")
        processed_key = event.get("processed_key")
        task_token = event.get("task_token")  # Optional - for Step Functions callback

        if not job_id:
            raise BDATriggerError("Missing required field: job_id")
        if not processed_key:
            raise BDATriggerError("Missing required field: processed_key")

        # Construct S3 URIs
        input_s3_uri = f"s3://{S3_BUCKET}/{processed_key}"
        output_s3_uri = f"s3://{S3_BUCKET}/{BDA_OUTPUT_PREFIX}/{job_id}/"

        log_event(
            "INFO",
            "Triggering BDA job",
            job_id=job_id,
            input_uri=input_s3_uri,
            output_uri=output_s3_uri,
        )

        # Invoke BDA job
        bda_invocation_id = invoke_bda_job(
            BDA_PROFILE_ARN,
            BDA_PROJECT_ARN,
            input_s3_uri,
            output_s3_uri,
            job_id,
        )

        # Update DynamoDB with BDA invocation ID and task token (if provided)
        update_job_with_bda_id(
            DYNAMODB_TABLE,
            job_id,
            bda_invocation_id,
            "BDA_PROCESSING",
            task_token,
        )

        # Return success response
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "bda_invocation_id": bda_invocation_id,
                "status": "BDA_PROCESSING",
            },
        }

        log_event(
            "INFO",
            "BDA job triggered successfully",
            job_id=job_id,
            bda_invocation_id=bda_invocation_id,
        )

        return response

    except BDATriggerError as e:
        log_event("ERROR", "BDA trigger error")
        return {
            "statusCode": 400,
            "error": "BDATriggerError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error during BDA trigger")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred while triggering BDA job",
        }
