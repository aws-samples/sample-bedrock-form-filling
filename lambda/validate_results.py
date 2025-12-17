"""
Lambda function to validate structured data results.

This function:
1. Validates JSON structure and schema compliance
2. Checks required fields (form_id, responses)
3. Validates field values against dynamic schema from DynamoDB
4. Updates job status to "VALIDATING"
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS client
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]


class ValidationError(Exception):
    """Custom exception for validation errors."""

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


def get_job_schema(table_name: str, job_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve form schema from DynamoDB job record.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier

    Returns:
        Parsed form schema dictionary, or None if not found

    Raises:
        ValidationError: If DynamoDB query fails
    """
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={"job_id": job_id})

        if "Item" not in response:
            raise ValidationError(f"Job not found: {job_id}")

        item = response["Item"]
        form_schema_json = item.get("form_schema")

        if not form_schema_json:
            log_event(
                "WARNING",
                "No form_schema found for job, skipping validation",
                job_id=job_id,
            )
            return None

        # Parse JSON string to dictionary
        form_schema = json.loads(form_schema_json)

        log_event(
            "INFO",
            "Retrieved form schema from DynamoDB",
            job_id=job_id,
            has_schema=True,
        )

        return form_schema

    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse form_schema JSON",
            job_id=job_id,
        )
        raise ValidationError(f"Invalid form_schema JSON: {e}") from e
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve job from DynamoDB",
            job_id=job_id,
        )
        raise ValidationError(f"DynamoDB query failed: {e}") from e


def validate_structure(
    data: Dict[str, Any], form_schema: Optional[Dict[str, Any]], job_id: str
) -> List[str]:
    """
    Validate basic JSON structure against dynamic schema.

    Args:
        data: Structured data to validate
        form_schema: Form schema from DynamoDB (or None to skip validation)
        job_id: Job identifier for logging

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Check for required top-level fields
    if "form_id" not in data:
        errors.append("Missing required field: form_id")

    if "responses" not in data:
        errors.append("Missing required field: responses")
        return errors  # Can't continue validation without responses

    # Skip schema-based validation if no schema provided
    if not form_schema:
        log_event(
            "INFO",
            "Skipping schema validation (no schema provided)",
            job_id=job_id,
        )
        return errors

    # Check responses structure
    responses = data.get("responses", {})
    if not isinstance(responses, dict):
        errors.append("Field 'responses' must be a dictionary")
        return errors

    # Validate against schema sections
    schema_sections = form_schema.get("sections", [])

    for section in schema_sections:
        section_id = section.get("id")
        if not section_id:
            continue

        # Check if section exists in responses
        if section_id not in responses:
            errors.append(f"Missing required section: {section_id}")
            continue

        section_responses = responses.get(section_id, {})
        if not isinstance(section_responses, dict):
            errors.append(f"Section '{section_id}' must be a dictionary")
            continue

        # Check required fields within section
        for field in section.get("fields", []):
            field_id = field.get("id")
            is_required = field.get("required", False)

            if is_required and field_id not in section_responses:
                errors.append(
                    f"Missing required field in section '{section_id}': {field_id}"
                )

    log_event(
        "INFO",
        "Structure validation completed",
        job_id=job_id,
        error_count=len(errors),
    )

    return errors


def validate_field_values(
    data: Dict[str, Any], form_schema: Optional[Dict[str, Any]], job_id: str
) -> List[str]:
    """
    Validate field values against dynamic schema constraints.

    Args:
        data: Structured data to validate
        form_schema: Form schema from DynamoDB (or None to skip validation)
        job_id: Job identifier for logging

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    # Skip validation if no schema provided
    if not form_schema:
        log_event(
            "INFO",
            "Skipping field value validation (no schema provided)",
            job_id=job_id,
        )
        return errors

    try:
        responses = data.get("responses", {})
        schema_sections = form_schema.get("sections", [])

        for section in schema_sections:
            section_id = section.get("id")
            if not section_id or section_id not in responses:
                continue

            section_responses = responses.get(section_id, {})

            for field in section.get("fields", []):
                field_id = field.get("id")
                field_type = field.get("type")
                field_options = field.get("options", [])

                if field_id not in section_responses:
                    continue

                field_value = section_responses.get(field_id)

                # Validate select/dropdown fields
                if field_type in ["select", "radio"] and field_options:
                    valid_options = [opt.get("value") for opt in field_options]
                    if field_value and field_value not in valid_options:
                        errors.append(
                            f"Invalid value for '{field_id}': must be one of {valid_options}, got '{field_value}'"
                        )

                # Validate text/textarea fields (non-empty check)
                if field_type in ["text", "textarea"]:
                    if field_value is not None:
                        if not isinstance(field_value, str):
                            errors.append(f"Field '{field_id}' must be a string")
                        elif not field_value.strip():
                            errors.append(f"Field '{field_id}' cannot be empty")

        log_event(
            "INFO",
            "Field value validation completed",
            job_id=job_id,
            error_count=len(errors),
        )

    except (KeyError, AttributeError, TypeError) as e:
        errors.append(f"Error during field validation: {e}")
        log_event(
            "WARNING",
            "Exception during field validation",
            job_id=job_id,
        )

    return errors


def update_job_status(
    table_name: str,
    job_id: str,
    status: str,
    is_valid: bool,
    validation_errors: Optional[List[str]] = None,
) -> None:
    """
    Update job status and validation results in DynamoDB.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        status: New job status
        is_valid: Whether validation passed
        validation_errors: List of validation errors (if any)

    Raises:
        ValidationError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    update_expression = "SET #status = :status, is_valid = :is_valid, updated_at = :timestamp"
    expression_values = {
        ":status": status,
        ":is_valid": is_valid,
        ":timestamp": timestamp,
    }

    # Add validation errors if present
    if validation_errors:
        update_expression += ", validation_errors = :errors"
        expression_values[":errors"] = validation_errors

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues=expression_values,
        )
        log_event(
            "INFO",
            "Job status updated",
            job_id=job_id,
            status=status,
            is_valid=is_valid,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update job status",
            job_id=job_id,
        )
        raise ValidationError(f"DynamoDB update failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for validating structured data.

    Expected event format:
    {
        "job_id": "uuid-string",
        "structured_data": {...}
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "is_valid": true|false,
            "validation_errors": [...],
            "status": "VALIDATING"
        }
    }
    """
    log_event("INFO", "Validate results Lambda invoked", job_id=event.get("job_id"),
              has_structured_data=bool(event.get("structured_data")))  # Avoid logging structured_data - contains extracted user data

    try:
        # Extract and validate input
        job_id = event.get("job_id")
        structured_data = event.get("structured_data")

        if not job_id:
            raise ValidationError("Missing required field: job_id")
        if not structured_data:
            raise ValidationError("Missing required field: structured_data")

        log_event("INFO", "Validating structured data", job_id=job_id)

        # Retrieve form schema from DynamoDB
        form_schema = get_job_schema(DYNAMODB_TABLE, job_id)

        # Perform validation
        structure_errors = validate_structure(structured_data, form_schema, job_id)
        value_errors = validate_field_values(structured_data, form_schema, job_id)

        # Combine all errors
        all_errors = structure_errors + value_errors
        is_valid = len(all_errors) == 0

        # Update job status in DynamoDB
        update_job_status(
            DYNAMODB_TABLE,
            job_id,
            "VALIDATING",
            is_valid,
            all_errors if all_errors else None,
        )

        # Return validation results
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "is_valid": is_valid,
                "validation_errors": all_errors if all_errors else [],
                "status": "VALIDATING",
            },
        }

        log_event(
            "INFO",
            "Validation completed",
            job_id=job_id,
            is_valid=is_valid,
            error_count=len(all_errors),
        )

        return response

    except ValidationError as e:
        log_event("ERROR", "Validation error")
        return {
            "statusCode": 400,
            "error": "ValidationError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error during validation")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred during validation",
        }
