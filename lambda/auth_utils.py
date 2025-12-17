"""
Authentication and authorization utilities for API handlers.
"""
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_user_id_from_event(event):
    """
    Extract user_id from Cognito authorizer claims in API Gateway event.

    Args:
        event: API Gateway event dict containing requestContext with authorizer claims

    Returns:
        str: User ID (Cognito 'sub' claim) or None if not found

    Raises:
        ValueError: If event structure is invalid or user_id cannot be extracted
    """
    try:
        # Extract Cognito claims from API Gateway event
        claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})

        # The 'sub' claim is the unique user identifier from Cognito
        user_id = claims.get("sub")

        if not user_id:
            logger.error("Missing 'sub' claim in Cognito authorizer")
            raise ValueError("User ID not found in authentication claims")

        logger.info(f"Extracted user_id from Cognito: {user_id}")
        return user_id

    except Exception as e:
        logger.error(f"Failed to extract user_id from event: {str(e)}")
        logger.error(f"Event structure: {event.get('requestContext', {})}")
        raise ValueError(f"Failed to extract user authentication: {str(e)}")
