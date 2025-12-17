"""
Lambda function to extract results from Bedrock Data Automation output.

This function:
1. Retrieves BDA metadata from S3
2. Extracts content (transcript, OCR text, etc.) from audio/video/document/image outputs
3. Stores content in S3 at transcripts/{job_id}/transcript.txt
4. Updates job status to "EXTRACTING_RESULTS"
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

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
BDA_OUTPUT_PREFIX = os.environ.get("BDA_OUTPUT_PREFIX", "bda-output")
TRANSCRIPT_PREFIX = os.environ.get("TRANSCRIPT_PREFIX", "transcripts")


class ExtractionError(Exception):
    """Custom exception for extraction errors."""

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


def retrieve_bda_metadata(bucket: str, job_id: str) -> Dict[str, Any]:
    """
    Retrieve BDA metadata JSON from S3.

    Args:
        bucket: S3 bucket name
        job_id: Job identifier

    Returns:
        Parsed BDA metadata as dictionary

    Raises:
        ExtractionError: If retrieval or parsing fails
    """
    # List objects to find the actual metadata file location
    # BDA creates: bda-output/{job_id}//{invocation_id}/job_metadata.json
    prefix = f"{BDA_OUTPUT_PREFIX}/{job_id}/"

    try:
        # List objects with the prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response:
            raise ExtractionError(f"No BDA output found for job {job_id}")

        # Find job_metadata.json
        metadata_key = None
        for obj in response['Contents']:
            if obj['Key'].endswith('job_metadata.json'):
                metadata_key = obj['Key']
                break

        if not metadata_key:
            raise ExtractionError(f"job_metadata.json not found in BDA output for job {job_id}")

        # Read metadata file
        response = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        content = response["Body"].read().decode("utf-8")
        metadata = json.loads(content)

        log_event(
            "INFO",
            "BDA metadata retrieved",
            job_id=job_id,
            metadata_key=metadata_key,
        )

        return metadata

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to retrieve BDA metadata",
            job_id=job_id,
            prefix=prefix,
        )
        raise ExtractionError(f"Failed to retrieve metadata: {e}") from e

    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse BDA metadata JSON",
            job_id=job_id,
            metadata_key=metadata_key if 'metadata_key' in locals() else prefix,
        )
        raise ExtractionError(f"Invalid JSON in metadata: {e}") from e


def extract_content_from_metadata(metadata: Dict[str, Any], job_id: str) -> tuple[str, str]:
    """
    Extract content (transcript, OCR text, etc.) from BDA metadata.

    BDA Structure:
    - job_metadata.json contains output_metadata with standard_output_path
    - standard_output_path points to result.json
    - result.json contains content in format-specific paths:
      - Audio: audio.transcript.representation.text
      - Video: video.transcript.representation.text
      - Document: document.content.representation.text
      - Image: image.content.representation.text

    Args:
        metadata: BDA job_metadata dictionary
        job_id: Job identifier for logging

    Returns:
        Tuple of (content_text, bda_output_key)

    Raises:
        ExtractionError: If extraction fails
    """
    try:
        # Get standard_output_path from job_metadata
        output_metadata = metadata.get("output_metadata", [])
        if not output_metadata:
            raise ExtractionError("No output_metadata found in job_metadata")

        # Get first asset's segment metadata
        segment_metadata = output_metadata[0].get("segment_metadata", [])
        if not segment_metadata:
            raise ExtractionError("No segment_metadata found")

        # Get standard output path (S3 URI)
        standard_output_path = segment_metadata[0].get("standard_output_path")
        if not standard_output_path:
            raise ExtractionError("No standard_output_path found")

        log_event(
            "INFO",
            "Found standard output path",
            job_id=job_id,
            output_path=standard_output_path,
        )

        # Parse S3 URI: s3://bucket/key
        if not standard_output_path.startswith("s3://"):
            raise ExtractionError(f"Invalid S3 URI: {standard_output_path}")

        s3_parts = standard_output_path[5:].split("/", 1)
        result_bucket = s3_parts[0]
        result_key = s3_parts[1]

        # Download result.json
        response = s3_client.get_object(Bucket=result_bucket, Key=result_key)
        result_content = response["Body"].read().decode("utf-8")
        result_data = json.loads(result_content)

        log_event(
            "INFO",
            "Downloaded result.json",
            job_id=job_id,
            result_key=result_key,
        )

        # Get semantic modality from metadata to determine extraction strategy
        semantic_modality = metadata.get("semantic_modality", "").upper()

        log_event(
            "INFO",
            "Detected modality",
            job_id=job_id,
            semantic_modality=semantic_modality,
        )

        # Extract content based on modality
        content = None
        content_type = None

        # Try video content first (must be before audio to avoid false positives)
        if semantic_modality == "VIDEO" and not content:
            video_data = result_data.get("video", {})

            # Extract audio transcript
            transcript = video_data.get("transcript", {}).get("representation", {}).get("text", "")

            # Extract video summary
            video_summary = video_data.get("summary", "")

            # Extract chapter summaries (chapters are at root level in result_data)
            chapters = result_data.get("chapters", [])
            chapter_summaries = []
            for i, chapter in enumerate(chapters, 1):
                chapter_summary = chapter.get("summary", "")
                if chapter_summary:
                    chapter_summaries.append(f"Chapter {i}:\n{chapter_summary}")

            # Extract text detected in video (OCR from frames in chapters)
            video_text_parts = []
            for chapter in chapters:
                frames = chapter.get("frames", [])
                for frame in frames:
                    text_words = frame.get("text_words", [])
                    for word in text_words:
                        if word.get("text"):
                            video_text_parts.append(word.get("text"))
            video_text = " ".join(video_text_parts)

            # Combine all video content
            if transcript or video_summary or chapter_summaries or video_text:
                parts = ["MODALITY: video"]  # Embed modality label
                if transcript:
                    parts.append(f"Audio Transcript:\n{transcript}")
                if video_summary:
                    parts.append(f"Video Summary:\n{video_summary}")
                if chapter_summaries:
                    parts.append(f"Chapter Summaries:\n" + "\n\n".join(chapter_summaries))
                if video_text:
                    parts.append(f"Text Detected in Video:\n{video_text}")
                content = "\n\n".join(parts)
                content_type = "video_content"

        # Try document content (page-level markdown, summaries, figure descriptions)
        if semantic_modality == "DOCUMENT" and not content:
            doc_data = result_data.get("document", {})
            pages_data = result_data.get("pages", [])
            entities_data = result_data.get("entities", [])

            # Extract document summaries (10-word and 250-word)
            doc_description = doc_data.get("description", "")  # 10-word summary
            doc_summary = doc_data.get("summary", "")  # 250-word summary

            # Extract page content (markdown format) with page numbers
            page_contents = []
            for page in pages_data:
                page_index = page.get("page_index", 0)
                detected_page_num = page.get("detected_page_number", page_index + 1)
                page_markdown = page.get("representation", {}).get("markdown", "")

                if page_markdown:
                    page_contents.append(f"=== Page {detected_page_num} ===\n\n{page_markdown}")

            # Extract figure descriptions from entities
            figure_descriptions = []
            for entity in entities_data:
                if entity.get("type") == "FIGURE":
                    figure_summary = entity.get("summary", "")
                    if figure_summary:
                        figure_descriptions.append(figure_summary)

            # Combine all document content per spec
            if doc_description or doc_summary or page_contents or figure_descriptions:
                parts = ["MODALITY: document"]  # Embed modality label
                if doc_description:
                    parts.append(f"Document Description (Brief):\n{doc_description}")
                if doc_summary:
                    parts.append(f"Document Summary:\n{doc_summary}")
                if figure_descriptions:
                    parts.append(f"Figure Descriptions:\n" + "\n\n".join([f"- {desc}" for desc in figure_descriptions]))
                if page_contents:
                    parts.append(f"{'='*50}\nDOCUMENT CONTENT BY PAGE\n{'='*50}\n\n" + "\n\n".join(page_contents))
                content = "\n\n".join(parts)
                content_type = "document_content"

        # Try image content (OCR or summary)
        if semantic_modality == "IMAGE" and not content:
            image_data = result_data.get("image", {})

            # Try summary first
            summary = image_data.get("summary", "")

            # Try OCR text words
            text_words = image_data.get("text_words", [])
            ocr_text = " ".join([word.get("text", "") for word in text_words if word.get("text")])

            # Combine summary and OCR text
            if summary or ocr_text:
                parts = ["MODALITY: image"]  # Embed modality label
                if summary:
                    parts.append(f"Image Summary:\n{summary}")
                if ocr_text:
                    parts.append(f"Extracted Text (OCR):\n{ocr_text}")
                content = "\n\n".join(parts)
                content_type = "image_content"

        # Try audio transcript as fallback (or if explicitly audio modality)
        if (semantic_modality == "AUDIO" or not content) and not content:
            audio_transcript = result_data.get("audio", {}).get("transcript", {}).get("representation", {}).get("text")
            if audio_transcript and audio_transcript.strip():
                content = f"MODALITY: audio\n\nAudio Transcript:\n{audio_transcript}"  # Embed modality label
                content_type = "audio_transcript"

        if not content:
            raise ExtractionError(f"No extractable content found in result.json for modality {semantic_modality}")

        log_event(
            "INFO",
            "Content extracted successfully",
            job_id=job_id,
            content_type=content_type,
            content_length=len(content),
            bda_output_key=result_key,
        )

        return content, result_key

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to download result.json",
            job_id=job_id,
        )
        raise ExtractionError(f"Failed to download result.json: {e}") from e

    except (KeyError, IndexError) as e:
        log_event(
            "ERROR",
            "Missing expected field in metadata",
            job_id=job_id,
        )
        raise ExtractionError(f"Invalid metadata structure: {e}") from e

    except json.JSONDecodeError as e:
        log_event(
            "ERROR",
            "Failed to parse result.json",
            job_id=job_id,
        )
        raise ExtractionError(f"Invalid JSON in result.json: {e}") from e


def store_content(bucket: str, job_id: str, content: str) -> str:
    """
    Store extracted content in S3.

    Args:
        bucket: S3 bucket name
        job_id: Job identifier
        content: Extracted content text to store

    Returns:
        S3 key where content was stored

    Raises:
        ExtractionError: If storage fails
    """
    content_key = f"{TRANSCRIPT_PREFIX}/{job_id}/transcript.txt"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=content_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )

        log_event(
            "INFO",
            "Content stored in S3",
            job_id=job_id,
            content_key=content_key,
            content_length=len(content),
        )

        return content_key

    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to store content",
            job_id=job_id,
            content_key=content_key,
        )
        raise ExtractionError(f"Failed to store content: {e}") from e


def update_job_status(
    table_name: str, job_id: str, status: str, transcript_key: str, bda_output_key: str = None
) -> None:
    """
    Update job status, transcript key, and BDA output key in DynamoDB.

    Args:
        table_name: Name of the DynamoDB table
        job_id: Job identifier
        status: New job status
        transcript_key: S3 key of stored transcript
        bda_output_key: S3 key of BDA result.json (optional)

    Raises:
        ExtractionError: If DynamoDB update fails
    """
    table = dynamodb.Table(table_name)
    timestamp = datetime.utcnow().isoformat()

    try:
        # Build update expression dynamically
        update_expr = "SET #status = :status, transcript_key = :transcript_key, updated_at = :timestamp"
        expr_values = {
            ":status": status,
            ":transcript_key": transcript_key,
            ":timestamp": timestamp,
        }

        if bda_output_key:
            update_expr += ", bda_output_key = :bda_output_key"
            expr_values[":bda_output_key"] = bda_output_key

        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues=expr_values,
        )
        log_event(
            "INFO",
            "Job status updated",
            job_id=job_id,
            status=status,
            transcript_key=transcript_key,
            bda_output_key=bda_output_key,
        )
    except ClientError as e:
        log_event(
            "ERROR",
            "Failed to update job status",
            job_id=job_id,
        )
        raise ExtractionError(f"DynamoDB update failed: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for extracting BDA results.

    Expected event format:
    {
        "job_id": "uuid-string",
        "bda_response": {...}
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "uuid-string",
            "transcript": "...",
            "transcript_key": "transcripts/job-id/transcript.txt",
            "status": "EXTRACTING_RESULTS"
        }
    }
    """
    log_event("INFO", "Extract results Lambda invoked")

    try:
        # Extract and validate input
        job_id = event.get("job_id")
        if not job_id:
            raise ExtractionError("Missing required field: job_id")

        log_event("INFO", "Extracting BDA results", job_id=job_id)

        # Retrieve BDA metadata from S3
        metadata = retrieve_bda_metadata(S3_BUCKET, job_id)

        # Extract content from metadata (transcript, OCR text, etc.) and get BDA output key
        content, bda_output_key = extract_content_from_metadata(metadata, job_id)

        # Get semantic modality from metadata for downstream processing
        semantic_modality = metadata.get("semantic_modality", "").upper()

        # Store content in S3
        content_key = store_content(S3_BUCKET, job_id, content)

        # Update job status in DynamoDB with all S3 keys
        update_job_status(DYNAMODB_TABLE, job_id, "EXTRACTING_RESULTS", content_key, bda_output_key)

        # Return success response
        response = {
            "statusCode": 200,
            "body": {
                "job_id": job_id,
                "content": content,  # Content now includes "MODALITY: xxx" prefix
                "transcript_key": content_key,  # Keep legacy key name for compatibility
                "status": "EXTRACTING_RESULTS",
            },
        }

        log_event(
            "INFO",
            "Results extraction completed successfully",
            job_id=job_id,
            content_key=content_key,
        )

        return response

    except ExtractionError as e:
        log_event("ERROR", "Extraction error")
        return {
            "statusCode": 400,
            "error": "ExtractionError",
            "message": str(e),
        }

    except Exception as e:
        log_event("ERROR", "Unexpected error during extraction")
        return {
            "statusCode": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred during results extraction",
        }
