"""
Lambda function to extract structured data from content using Claude.

This function:
1. Uses Claude API to analyze extracted content (transcript, OCR text, etc.)
2. Extracts structured data based on form schema
3. Stores results in S3 at results/{job_id}/structured-data.json
4. Updates job status to "PROCESSING_STRUCTURED_DATA"
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
bedrock_runtime = boto3.client("bedrock-runtime")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Environment variables
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
RESULTS_PREFIX = os.environ.get("RESULTS_PREFIX", "results")
CLAUDE_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.amazon.nova-lite-v1:0")

# Simple form schema for media analysis (NEW FLAT FORMAT)
FORM_SCHEMA = {
    "form_id": "simple_media_analysis_v1",
    "fields": [
        {
            "field_id": "content_type",
            "field_name": "Content Type",
            "field_type": "select",
            "options": ["audio", "video", "document", "image"],
        },
        {
            "field_id": "main_topics",
            "field_name": "Main Topics or Themes",
            "field_type": "text",
        },
        {
            "field_id": "summary",
            "field_name": "Content Summary",
            "field_type": "text",
        },
        {
            "field_id": "sentiment",
            "field_name": "Overall Sentiment",
            "field_type": "radio",
            "options": ["positive", "neutral", "negative"],
        },
    ],
}


class StructuredDataError(Exception):
    """Custom exception for structured data extraction errors."""

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


def get_form_schema_from_db(job_id: str) -> Dict[str, Any]:
    """
    Retrieve form schema from DynamoDB if custom schema was provided.

    Args:
        job_id: Job identifier

    Returns:
        Form schema dictionary, or None if not found

    Raises:
        StructuredDataError: If DynamoDB read fails
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        response = table.get_item(Key={"job_id": job_id})

        if "Item" in response and "form_schema" in response["Item"]:
            form_schema_str = response["Item"]["form_schema"]
            return json.loads(form_schema_str)

        return None

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve form schema from DynamoDB",
            job_id=job_id,
        )
        raise StructuredDataError(f"Failed to retrieve form schema: {e}") from e
    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse form schema JSON",
            job_id=job_id,
        )
        raise StructuredDataError(f"Invalid form schema JSON: {e}") from e


def get_definitions_from_db(job_id: str) -> str:
    """
    Retrieve industry-specific definitions from DynamoDB if provided.

    Args:
        job_id: Job identifier

    Returns:
        Definitions string, or None if not found
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        response = table.get_item(Key={"job_id": job_id})

        if "Item" in response and "definitions" in response["Item"]:
            return response["Item"]["definitions"]

        return None

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve definitions from DynamoDB",
            job_id=job_id,
        )
        # Don't raise - definitions are optional
        return None


def get_pre_filled_values_from_db(job_id: str) -> Dict[str, Any]:
    """
    Retrieve pre-filled field values from DynamoDB if provided.

    Args:
        job_id: Job identifier

    Returns:
        Pre-filled values dictionary, or None if not found
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        response = table.get_item(Key={"job_id": job_id})

        if "Item" in response and "pre_filled_values" in response["Item"]:
            pre_filled_str = response["Item"]["pre_filled_values"]
            return json.loads(pre_filled_str)

        return None

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve pre-filled values from DynamoDB",
            job_id=job_id,
        )
        # Don't raise - pre-filled values are optional
        return None
    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse pre-filled values JSON",
            job_id=job_id,
        )
        # Don't raise - pre-filled values are optional
        return None


def build_prompt_from_schema(
    schema: Dict[str, Any],
    content: str,
    pre_filled_values: Dict[str, Any] = None,
    definitions: str = None
) -> str:
    """
    Build Claude prompt dynamically from form schema with support for definitions and pre-filled values.
    Supports both NEW flat format (fields array) and OLD nested format (sections array) for backward compatibility.

    Args:
        schema: Form schema dictionary
        content: Extracted content (includes "MODALITY: xxx" prefix)
        pre_filled_values: Optional pre-filled field values to preserve
        definitions: Optional industry-specific definitions for guidance

    Returns:
        Formatted prompt string
    """
    form_id = schema.get("form_id", "custom_form")

    # Support both OLD (sections) and NEW (fields) formats
    # OLD format: {"form_id": "...", "sections": [{"section_id": "...", "fields": [...]}]}
    # NEW format: {"form_id": "...", "fields": [...]}
    is_old_format = "sections" in schema

    # Filter schema to remove pre-filled fields BEFORE building prompt
    # This reduces token usage and prevents LLM from seeing/modifying pre-filled values
    filtered_schema = filter_schema_fields(schema, pre_filled_values)

    # Build list of fields that need extraction (from filtered schema)
    extract_fields = []

    if is_old_format:
        # OLD FORMAT: nested sections
        sections = filtered_schema.get("sections", [])
        for section in sections:
            fields = section.get("fields", [])

            for field in fields:
                field_id = field.get("field_id")
                field_type = field.get("field_type", "text")
                options = field.get("options", [])

                if field_type in ["select", "radio"] and options:
                    extract_fields.append(f'"{field_id}": "<select one: {", ".join(options)}>"')
                else:
                    extract_fields.append(f'"{field_id}": "<extract from content>"')
    else:
        # NEW FORMAT: flat fields array
        fields = filtered_schema.get("fields", [])

        for field in fields:
            field_id = field.get("field_id")
            field_type = field.get("field_type", "text")
            options = field.get("options", [])

            if field_type in ["select", "radio"] and options:
                extract_fields.append(f'"{field_id}": "<select one: {", ".join(options)}>"')
            else:
                extract_fields.append(f'"{field_id}": "<extract from content>"')

    # Build prompt parts
    prompt_parts = []

    # 1. Industry definitions (if provided)
    if definitions:
        prompt_parts.append(f"""Industry-Specific Definitions and Context:
{definitions}

Use these definitions to accurately interpret the content and extract information.""")

    # 2. Main instruction
    # Content already includes "MODALITY: xxx" prefix for context
    prompt_parts.append(f"""Analyze the following extracted content and extract structured information.

Content:
{content}""")

    # 3. Fields to extract (only non-pre-filled fields)
    if extract_fields:
        extract_json = ",\n            ".join(extract_fields)
        prompt_parts.append(f"""
Extract the following information from the content:
{extract_json}""")

    # 4. Output format - use ORIGINAL schema to determine format and build complete response structure
    # We need all fields (including pre-filled) in the response format, but LLM only sees filtered fields
    all_fields_for_format = []

    if is_old_format:
        # OLD FORMAT: nested sections - build field list from ORIGINAL schema
        sections = schema.get("sections", [])
        for section in sections:
            section_id = section.get("section_id", "content")
            fields = section.get("fields", [])

            for field in fields:
                field_id = field.get("field_id")
                field_type = field.get("field_type", "text")
                options = field.get("options", [])

                # Check if pre-filled
                is_prefilled = (
                    pre_filled_values and
                    section_id in pre_filled_values and
                    field_id in pre_filled_values[section_id]
                )

                if is_prefilled:
                    value = pre_filled_values[section_id][field_id]
                    all_fields_for_format.append(f'"{field_id}": "{value}"')
                else:
                    if field_type in ["select", "radio"] and options:
                        all_fields_for_format.append(f'"{field_id}": "<select one: {", ".join(options)}>"')
                    else:
                        all_fields_for_format.append(f'"{field_id}": "<extracted value>"')
    else:
        # NEW FORMAT: flat fields - build field list from ORIGINAL schema
        fields = schema.get("fields", [])

        for field in fields:
            field_id = field.get("field_id")
            field_type = field.get("field_type", "text")
            options = field.get("options", [])

            # Check if pre-filled
            is_prefilled = (
                pre_filled_values and
                field_id in pre_filled_values
            )

            if is_prefilled:
                value = pre_filled_values[field_id]
                all_fields_for_format.append(f'"{field_id}": "{value}"')
            else:
                if field_type in ["select", "radio"] and options:
                    all_fields_for_format.append(f'"{field_id}": "<select one: {", ".join(options)}>"')
                else:
                    all_fields_for_format.append(f'"{field_id}": "<extracted value>"')

    all_fields_json = ",\n            ".join(all_fields_for_format)

    # Generate output format based on schema type
    if is_old_format:
        # OLD FORMAT: nested responses by section
        section_id = schema.get("sections", [{}])[0].get("section_id", "content")
        prompt_parts.append(f"""
Return ONLY valid JSON in this exact format:
{{
    "form_id": "{form_id}",
    "responses": {{
        "{section_id}": {{
            {all_fields_json}
        }}
    }}
}}

Important:
- Return ONLY the JSON, no other text
- Extract all fields from the content
- Use the definitions provided to interpret industry-specific terms
- If a field cannot be determined from the content, use "unknown" or best approximation""")
    else:
        # NEW FORMAT: flat responses object
        prompt_parts.append(f"""
Return ONLY valid JSON in this exact format:
{{
    "form_id": "{form_id}",
    "responses": {{
        {all_fields_json}
    }}
}}

Important:
- Return ONLY the JSON, no other text
- Extract all fields from the content
- Use the definitions provided to interpret industry-specific terms
- If a field cannot be determined from the content, use "unknown" or best approximation""")

    return "\n\n".join(prompt_parts)


def filter_schema_fields(
    schema: Dict[str, Any],
    pre_filled_values: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Remove pre-filled fields from schema to create filtered schema for LLM.

    Only fields that need LLM extraction should remain in the filtered schema.
    Pre-filled fields are excluded to save tokens and prevent LLM modification.

    Args:
        schema: Full form schema (OLD or NEW format)
        pre_filled_values: Pre-filled field values (OLD or NEW format)
            - OLD format: {section_id: {field_id: value}}
            - NEW format: {field_id: value}

    Returns:
        Filtered schema with only fields that need extraction.
        Returns original schema if pre_filled_values is None or empty.

    Examples:
        NEW format:
        schema = {"form_id": "test", "fields": [{"field_id": "a"}, {"field_id": "b"}]}
        pre_filled = {"a": "value"}
        Result: {"form_id": "test", "fields": [{"field_id": "b"}]}

        OLD format:
        schema = {"form_id": "test", "sections": [{"section_id": "s1", "fields": [{"field_id": "a"}]}]}
        pre_filled = {"s1": {"a": "value"}}
        Result: {"form_id": "test", "sections": [{"section_id": "s1", "fields": []}]}
    """
    # Return original schema if pre_filled_values is None or empty
    if not pre_filled_values:
        return schema

    # Detect format: OLD format has "sections", NEW format has "fields"
    is_old_format = "sections" in schema

    # Create a deep copy to avoid mutating the original
    import copy
    filtered_schema = copy.deepcopy(schema)

    if is_old_format:
        # OLD FORMAT: Process nested sections
        for section in filtered_schema.get("sections", []):
            section_id = section.get("section_id", "")
            section_pre_filled = pre_filled_values.get(section_id, {})

            # Filter fields for this section
            original_fields = section.get("fields", [])
            filtered_fields = [
                field for field in original_fields
                if field.get("field_id") not in section_pre_filled
            ]
            section["fields"] = filtered_fields
    else:
        # NEW FORMAT: Process flat fields array
        original_fields = filtered_schema.get("fields", [])
        filtered_fields = [
            field for field in original_fields
            if field.get("field_id") not in pre_filled_values
        ]
        filtered_schema["fields"] = filtered_fields

    return filtered_schema


def merge_llm_with_prefilled(
    llm_responses: Dict[str, Any],
    pre_filled_values: Dict[str, Any],
    schema_format: str
) -> Dict[str, Any]:
    """
    Merge LLM-extracted fields with pre-filled values.

    Pre-filled values always take precedence over LLM responses in case of conflicts.

    Args:
        llm_responses: LLM response dict (responses object only, NOT full form)
            - OLD format: {section_id: {field_id: value}}
            - NEW format: {field_id: value}
        pre_filled_values: Pre-filled field values (same format as llm_responses)
        schema_format: "old" (nested sections) or "new" (flat fields)

    Returns:
        Complete responses dict with both pre-filled and LLM-extracted values.
        Format matches schema_format parameter.

    Examples:
        NEW format:
        llm_responses = {"b": "llm_value", "c": "llm_value2"}
        pre_filled = {"a": "prefill_value"}
        Result: {"a": "prefill_value", "b": "llm_value", "c": "llm_value2"}

        OLD format:
        llm_responses = {"content": {"b": "llm_value"}}
        pre_filled = {"content": {"a": "prefill_value"}}
        Result: {"content": {"a": "prefill_value", "b": "llm_value"}}

        Conflict (pre-filled wins):
        llm_responses = {"a": "llm_value"}
        pre_filled = {"a": "prefill_value"}
        Result: {"a": "prefill_value"}  # pre-filled takes precedence
    """
    # Handle None/empty pre_filled_values
    if not pre_filled_values:
        return llm_responses if llm_responses else {}

    # Handle None/empty llm_responses
    if not llm_responses:
        return pre_filled_values if pre_filled_values else {}

    # Handle NEW format (flat fields)
    if schema_format == "new":
        # Merge dicts with pre_filled_values taking precedence
        # Dict order matters: later keys override earlier ones
        merged = {**llm_responses, **pre_filled_values}
        return merged

    # Handle OLD format (nested sections)
    if schema_format == "old":
        merged = {}

        # Get all unique section IDs from both dicts
        all_sections = set(llm_responses.keys()) | set(pre_filled_values.keys())

        for section_id in all_sections:
            llm_section = llm_responses.get(section_id, {})
            prefill_section = pre_filled_values.get(section_id, {})

            # Merge section fields with pre_filled taking precedence
            merged[section_id] = {**llm_section, **prefill_section}

        return merged

    # Invalid schema_format
    raise ValueError(f"Invalid schema_format: {schema_format}. Must be 'old' or 'new'.")


def invoke_claude(
    content: str,
    job_id: str,
    form_schema: Dict[str, Any] = None,
    pre_filled_values: Dict[str, Any] = None,
    definitions: str = None
) -> Dict[str, Any]:
    """
    Invoke Claude API to extract structured data from content.

    Args:
        content: Extracted content text (includes "MODALITY: xxx" prefix)
        job_id: Job identifier for logging
        form_schema: Optional custom form schema
        pre_filled_values: Optional pre-filled field values to preserve
        definitions: Optional industry-specific definitions for guidance

    Returns:
        Structured data dictionary

    Raises:
        StructuredDataError: If Claude invocation fails
    """
    # Determine the schema to use
    schema = form_schema if form_schema else FORM_SCHEMA

    # Detect schema format for later merging
    schema_format = "old" if "sections" in schema else "new"

    # Build prompt from schema
    prompt = build_prompt_from_schema(
        schema=schema,
        content=content,
        pre_filled_values=pre_filled_values,
        definitions=definitions
    )

    try:
        # Invoke Claude via Bedrock
        response = bedrock_runtime.converse(
            modelId=CLAUDE_MODEL_ID,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": prompt}],
                }
            ],
            inferenceConfig={
                "maxTokens": 1000,
                "temperature": 0.0,
            },
        )

        # Extract response text
        output_message = response.get("output", {}).get("message", {})
        content_blocks = output_message.get("content", [])

        if not content_blocks:
            raise StructuredDataError("Empty response from Claude")

        response_text = content_blocks[0].get("text", "")

        # Parse JSON response
        try:
            structured_data = json.loads(response_text)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
                structured_data = json.loads(response_text)
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
                structured_data = json.loads(response_text)
            else:
                raise

        # Merge LLM responses with pre-filled values
        if pre_filled_values:
            llm_responses = structured_data.get("responses", {})
            merged_responses = merge_llm_with_prefilled(
                llm_responses=llm_responses,
                pre_filled_values=pre_filled_values,
                schema_format=schema_format
            )
            structured_data["responses"] = merged_responses

        log_event(
            "INFO",
            "Claude API invoked successfully",
            job_id=job_id,
            input_tokens=response.get("usage", {}).get("inputTokens"),
            output_tokens=response.get("usage", {}).get("outputTokens"),
        )

        return structured_data

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to invoke Claude API",
            job_id=job_id,
        )
        raise StructuredDataError(f"Claude API invocation failed: {e}") from e

    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse Claude response as JSON",
            job_id=job_id,
            response_length=len(response_text),
            has_markdown_blocks="```" in response_text,
        )  # Avoid logging response_text - may contain extracted user data
        raise StructuredDataError(f"Invalid JSON in Claude response: {e}") from e


def store_structured_data(bucket: str, job_id: str, data: Dict[str, Any]) -> str:
    """
    Store structured data in S3.

    Args:
        bucket: S3 bucket name
        job_id: Job identifier
        data: Structured data to store

    Returns:
        S3 key where data was stored

    Raises:
        StructuredDataError: If storage fails
    """
    structured_key = f"{RESULTS_PREFIX}/{job_id}/structured-data.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=structured_key,
            Body=json.dumps(data, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        log_event(
            "INFO",
            "Structured data stored in S3",
            job_id=job_id,
            structured_key=structured_key,
        )

        return structured_key

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to store structured data",
            job_id=job_id,
            structured_key=structured_key,
        )
        raise StructuredDataError(f"Failed to store structured data: {e}") from e


def update_job_status(
    table_name: str, job_id: str, status: str, structured_key: str
) -> None:
    """
    Update job status and structured data key in DynamoDB.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        status: New job status
        structured_key: S3 key of stored structured data

    Raises:
        StructuredDataError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, structured_data_key = :structured_key, updated_at = :timestamp",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": status,
                ":structured_key": structured_key,
                ":timestamp": timestamp,
            },
        )
        log_event(
            "INFO",
            "Job status updated",
            job_id=job_id,
            status=status,
            structured_key=structured_key,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update job status",
            job_id=job_id,
        )
        raise StructuredDataError(f"DynamoDB update failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for extracting structured data.

    Expected event format:
    {
        "job_id": "uuid-string",
        "content": "extracted content text..." (or "transcript" for backward compatibility)
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "structured_data": {...},
            "structured_key": "results/job-id/structured-data.json",
            "status": "PROCESSING_STRUCTURED_DATA"
        }
    }
    """
    log_event("INFO", "Structured data Lambda invoked", job_id=event.get("job_id"),
              has_content=bool(event.get("content") or event.get("transcript")))  # Avoid logging content - contains user media text

    try:
        # Extract and validate input
        job_id = event.get("job_id")
        # Support both "content" and "transcript" (legacy) parameter names
        content = event.get("content") or event.get("transcript")

        if not job_id:
            raise StructuredDataError("Missing required field: job_id")
        if not content:
            raise StructuredDataError("Missing required field: content or transcript")

        log_event(
            "INFO",
            "Processing structured data extraction",
            job_id=job_id,
            content_length=len(content),
        )

        # Retrieve custom form schema if available
        form_schema = get_form_schema_from_db(job_id)
        if form_schema:
            log_event(
                "INFO",
                "Using custom form schema",
                job_id=job_id,
                form_id=form_schema.get("form_id"),
            )

        # Retrieve definitions if available
        definitions = get_definitions_from_db(job_id)
        if definitions:
            log_event(
                "INFO",
                "Using custom definitions",
                job_id=job_id,
                definitions_length=len(definitions),
            )

        # Retrieve pre-filled values if available
        pre_filled_values = get_pre_filled_values_from_db(job_id)
        if pre_filled_values:
            log_event(
                "INFO",
                "Using pre-filled values",
                job_id=job_id,
                prefilled_sections=list(pre_filled_values.keys()),
            )

        # Invoke Claude to extract structured data
        structured_data = invoke_claude(
            content, job_id, form_schema, pre_filled_values, definitions
        )

        # Store structured data in S3
        structured_key = store_structured_data(S3_BUCKET, job_id, structured_data)

        # Update job status in DynamoDB
        update_job_status(
            DYNAMODB_TABLE, job_id, "PROCESSING_STRUCTURED_DATA", structured_key
        )

        # Return success response
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "structured_data": structured_data,
                "structured_key": structured_key,
                "status": "PROCESSING_STRUCTURED_DATA",
            },
        }

        log_event(
            "INFO",
            "Structured data extraction completed successfully",
            job_id=job_id,
            structured_key=structured_key,
        )

        return response

    except StructuredDataError as e:
        log_event("ERROR", "Structured data extraction error")
        return {
            "statusCode": 400,
            "error": "StructuredDataError",
            "message": str(e),
        }

    except Exception as e:
        log_event(
            "ERROR", "Unexpected error during structured data extraction"
        )
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred during structured data extraction",
        }
