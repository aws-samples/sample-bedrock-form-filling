"""
Lambda function to handle BDA completion EventBridge events.

This function:
1. Receives EventBridge events when BDA jobs complete
2. Retrieves task token from DynamoDB with retry logic
3. Calls Step Functions SendTaskSuccess/SendTaskFailure to resume execution
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb")
stepfunctions = boto3.client("stepfunctions")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]


class EventBridgeHandlerError(Exception):
    """Custom exception for EventBridge handler errors."""

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


def get_task_token(table_name: str, invocation_arn: str) -> tuple:
    """
    Retrieve task token and job_id from DynamoDB using GSI with retry logic.

    This function implements exponential backoff retry to handle GSI eventual consistency.
    BDA completion events may arrive before the GSI is fully consistent with the base table.

    Args:
        table_name: Name of the DynamoDB table
        invocation_arn: BDA invocation ARN from EventBridge event

    Returns:
        Tuple of (task_token, job_id)

    Raises:
        EventBridgeHandlerError: If task token not found after all retries
    """
    table = dynamodb.Table(table_name)

    # Retry configuration: exponential backoff
    max_attempts = 5
    base_delay = 0.5  # 500ms
    max_delay = 8.0  # 8 seconds

    for attempt in range(1, max_attempts + 1):
        try:
            # Query GSI using bda_invocation_id
            response = table.query(
                IndexName="bda-invocation-index",
                KeyConditionExpression="bda_invocation_id = :invocation_id",
                ExpressionAttributeValues={":invocation_id": invocation_arn},
            )

            items = response.get("Items", [])
            if items:
                item = items[0]
                task_token = item.get("task_token")
                job_id = item.get("job_id")

                if not task_token:
                    raise EventBridgeHandlerError(
                        f"Task token not found for job: {job_id}"
                    )

                log_event(
                    "INFO",
                    "Retrieved task token from DynamoDB",
                    job_id=job_id,
                    invocation_arn=invocation_arn,
                    attempt=attempt,
                )

                return task_token, job_id

            # No items found - may be GSI consistency issue
            if attempt < max_attempts:
                # Calculate exponential backoff delay
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

                log_event(
                    "WARNING",
                    "No job found in GSI, retrying after delay (possible GSI consistency issue)",
                    invocation_arn=invocation_arn,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    retry_delay_seconds=delay,
                )

                time.sleep(delay)
            else:
                # Final attempt failed
                raise EventBridgeHandlerError(
                    f"No job found for invocation ARN after {max_attempts} attempts: {invocation_arn}"
                )

        except ClientError as e:
            log_event(
                "ERROR",
                "Failed to query DynamoDB",
                invocation_arn=invocation_arn,
                attempt=attempt,
            )
            raise EventBridgeHandlerError(f"DynamoDB query failed: {e}") from e

    # Should never reach here, but for safety
    raise EventBridgeHandlerError(
        f"Unexpected error: exhausted retries without finding job for {invocation_arn}"
    )


def send_task_success(task_token: str, job_id: str, event_detail: Dict) -> None:
    """
    Send task success callback to Step Functions.

    Args:
        task_token: Step Functions task token
        job_id: Job identifier
        event_detail: EventBridge event detail object

    Raises:
        EventBridgeHandlerError: If callback fails
    """
    try:
        output_data = {
            "job_id": job_id,
            "bda_invocation_id": event_detail.get("invocationArn"),
            "status": "SUCCESS",
            "bda_response": event_detail,
        }

        stepfunctions.send_task_success(
            taskToken=task_token, output=json.dumps(output_data)
        )

        log_event(
            "INFO",
            "Sent task success to Step Functions",
            job_id=job_id,
            invocation_arn=event_detail.get("invocationArn"),
        )

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to send task success",
            job_id=job_id,
        )
        raise EventBridgeHandlerError(f"SendTaskSuccess failed: {e}") from e


def send_task_failure(task_token: str, job_id: str, event_detail: Dict) -> None:
    """
    Send task failure callback to Step Functions.

    Args:
        task_token: Step Functions task token
        job_id: Job identifier
        event_detail: EventBridge event detail object

    Raises:
        EventBridgeHandlerError: If callback fails
    """
    try:
        error_message = event_detail.get("error_message", "BDA job failed")

        stepfunctions.send_task_failure(
            taskToken=task_token,
            error="BDAJobFailed",
            cause=json.dumps(
                {
                    "job_id": job_id,
                    "invocation_arn": event_detail.get("invocationArn"),
                    "error_message": error_message,
                    "event_detail": event_detail,
                }
            ),
        )

        log_event(
            "INFO",
            "Sent task failure to Step Functions",
            job_id=job_id,
            invocation_arn=event_detail.get("invocationArn"),
            error_message=error_message,
        )

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to send task failure",
            job_id=job_id,
        )
        raise EventBridgeHandlerError(f"SendTaskFailure failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for BDA EventBridge completion events.

    Expected event format:
    {
      "version": "0",
      "id": "event-id",
      "detail-type": "Bedrock Data Automation Job Succeeded",
      "source": "aws.bedrock",
      "detail": {
        "invocationArn": "arn:aws:bedrock:...",
        "job_status": "SUCCESS",
        "output_s3_location": {...},
        "error_message": ""
      }
    }

    Returns:
    {
        "statusCode": 200,
        "message": "Callback sent successfully"
    }
    """
    log_event("INFO", "EventBridge handler invoked")

    try:
        # Extract event details
        detail_type = event.get("detail-type")
        detail = event.get("detail", {})

        # BDA event provides job_id (short form) - construct full ARN
        job_id = detail.get("job_id")
        if not job_id:
            raise EventBridgeHandlerError("Missing job_id in event detail")

        # Construct full invocation ARN from job_id
        # Format: arn:aws:bedrock:region:account:data-automation-invocation/job_id
        region = event.get("region", "us-east-1")
        account = event.get("account")
        invocation_arn = f"arn:aws:bedrock:{region}:{account}:data-automation-invocation/{job_id}"

        if not invocation_arn:
            raise EventBridgeHandlerError("Failed to construct invocation ARN")

        log_event(
            "INFO",
            "Processing BDA completion event",
            detail_type=detail_type,
            invocation_arn=invocation_arn,
        )

        # Retrieve task token from DynamoDB
        task_token, job_id = get_task_token(DYNAMODB_TABLE, invocation_arn)

        # Send appropriate callback based on event type
        if "Succeeded" in detail_type:
            send_task_success(task_token, job_id, detail)
        else:
            # Handle both client and service errors
            send_task_failure(task_token, job_id, detail)

        return {
            "statusCode": 200,
            "message": "Callback sent successfully",
            "job_id": job_id,
        }

    except EventBridgeHandlerError as e:
        log_event("ERROR", "EventBridge handler error")
        return {
            "statusCode": 400,
            "error": "EventBridgeHandlerError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error in EventBridge handler")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred while processing BDA event",
        }
