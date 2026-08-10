import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

EVENTS_TABLE = os.environ["EVENTS_TABLE"]
REGISTRATIONS_TABLE = os.environ["REGISTRATIONS_TABLE"]

dynamodb_resource = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

events_table = dynamodb_resource.Table(EVENTS_TABLE)
serializer = TypeSerializer()

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def build_response(status_code: int, payload: dict) -> dict:
    """Return a consistent API Gateway-compatible response."""
    return {
        "statusCode": status_code,
            "headers": {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
},
        "body": json.dumps(payload)
    }


def parse_request_body(event: dict) -> dict:
    """Parse the API Gateway body or a direct Lambda test body."""
    body = event.get("body", {})

    if body is None:
        return {}

    if isinstance(body, str):
        return json.loads(body)

    if isinstance(body, dict):
        return body

    raise ValueError("Request body must be a JSON object.")


def extract_event_id(event: dict) -> str:
    """Read and clean event_id from API Gateway path parameters."""
    path_parameters = event.get("pathParameters") or {}
    event_id = path_parameters.get("event_id")

    if not isinstance(event_id, str):
        return ""

    return event_id.strip()


def is_valid_email(email: str) -> bool:
    """Apply basic email-format validation."""
    return bool(EMAIL_PATTERN.fullmatch(email))


def calculate_event_status(
    registered_count: int,
    capacity: int
) -> str:
    """
    Derive event availability.

    AVAILABLE: below 80% occupancy
    LIMITED: 80% or more occupied
    SOLD_OUT: capacity reached
    """
    if registered_count >= capacity:
        return "SOLD_OUT"

    if registered_count * 100 >= capacity * 80:
        return "LIMITED"

    return "AVAILABLE"


def serialize_item(item: dict) -> dict:
    """Convert a Python dictionary into DynamoDB attribute values."""
    return {
        key: serializer.serialize(value)
        for key, value in item.items()
    }


def get_event(event_id: str):
    """Retrieve the latest event state using a consistent read."""
    response = events_table.get_item(
        Key={"event_id": event_id},
        ConsistentRead=True
    )

    return response.get("Item")


def cancellation_code(reasons: list, position: int) -> str:
    """Safely retrieve a transaction cancellation-reason code."""
    if position >= len(reasons):
        return ""

    reason = reasons[position] or {}
    return reason.get("Code", "")


def lambda_handler(event, context):
    try:
        if not isinstance(event, dict):
            return build_response(
                400,
                {"message": "Invalid request format."}
            )

        event_id = extract_event_id(event)

        if not event_id:
            return build_response(
                400,
                {"message": "event_id is required in pathParameters."}
            )

        try:
            request_body = parse_request_body(event)
        except (json.JSONDecodeError, TypeError, ValueError):
            return build_response(
                400,
                {"message": "Request body must contain valid JSON."}
            )

        email = request_body.get("email")

        if not isinstance(email, str) or not email.strip():
            return build_response(
                400,
                {"message": "A participant email address is required."}
            )

        normalized_email = email.strip().lower()

        if not is_valid_email(normalized_email):
            return build_response(
                400,
                {"message": "A valid participant email address is required."}
            )

        registration_id = (
            f"REG-{uuid.uuid4().hex[:12].upper()}"
        )

        registered_at = datetime.now(
            timezone.utc
        ).isoformat().replace("+00:00", "Z")

        registration_item = {
            "event_id": event_id,
            "email": normalized_email,
            "registration_id": registration_id,
            "registered_at": registered_at,
            "status": "CONFIRMED"
        }

        # Retry when another registration changes the event count
        # between the consistent read and the transaction.
        for attempt in range(3):
            event_item = get_event(event_id)

            if not event_item:
                return build_response(
                    404,
                    {"message": "The requested event was not found."}
                )

            try:
                capacity = int(event_item.get("capacity", 0))
                current_count = int(
                    event_item.get("registered_count", 0)
                )
            except (TypeError, ValueError):
                logger.error(
                    "Invalid event capacity data. event_id=%s",
                    event_id
                )

                return build_response(
                    500,
                    {"message": "An unexpected backend error occurred."}
                )

            if capacity <= 0:
                logger.error(
                    "Event has invalid capacity. event_id=%s",
                    event_id
                )

                return build_response(
                    500,
                    {"message": "An unexpected backend error occurred."}
                )

            if current_count >= capacity:
                logger.info(
                    "Registration rejected because event is sold out. "
                    "event_id=%s",
                    event_id
                )

                return build_response(
                    409,
                    {"message": "The event is sold out."}
                )

            new_count = current_count + 1
            new_event_status = calculate_event_status(
                new_count,
                capacity
            )

            try:
                dynamodb_client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": REGISTRATIONS_TABLE,
                                "Item": serialize_item(
                                    registration_item
                                ),
                                "ConditionExpression": (
                                    "attribute_not_exists(event_id) "
                                    "AND attribute_not_exists(email)"
                                ),
                                "ReturnValuesOnConditionCheckFailure": (
                                    "ALL_OLD"
                                )
                            }
                        },
                        {
                            "Update": {
                                "TableName": EVENTS_TABLE,
                                "Key": {
                                    "event_id": {
                                        "S": event_id
                                    }
                                },
                                "UpdateExpression": (
                                    "SET registered_count = "
                                    "registered_count + :one, "
                                    "#event_status = :new_status"
                                ),
                                "ConditionExpression": (
                                    "attribute_exists(event_id) "
                                    "AND registered_count = "
                                    ":expected_count "
                                    "AND registered_count < :capacity"
                                ),
                                "ExpressionAttributeNames": {
                                    "#event_status": "status"
                                },
                                "ExpressionAttributeValues": {
                                    ":one": {
                                        "N": "1"
                                    },
                                    ":expected_count": {
                                        "N": str(current_count)
                                    },
                                    ":capacity": {
                                        "N": str(capacity)
                                    },
                                    ":new_status": {
                                        "S": new_event_status
                                    }
                                },
                                "ReturnValuesOnConditionCheckFailure": (
                                    "ALL_OLD"
                                )
                            }
                        }
                    ]
                )

                logger.info(
                    "Registration completed. "
                    "event_id=%s registration_id=%s",
                    event_id,
                    registration_id
                )

                return build_response(
                    201,
                    {
                        "message": (
                            "Registration completed successfully."
                        ),
                        "registration": {
                            "registration_id": registration_id,
                            "event_id": event_id,
                            "email": normalized_email,
                            "registered_at": registered_at,
                            "status": "CONFIRMED"
                        },
                        "event": {
                            "capacity": capacity,
                            "registered_count": new_count,
                            "status": new_event_status
                        }
                    }
                )

            except (
                dynamodb_client.exceptions.TransactionCanceledException
            ) as error:
                reasons = error.response.get(
                    "CancellationReasons",
                    []
                )

                registration_failure = cancellation_code(
                    reasons,
                    0
                )

                event_update_failure = cancellation_code(
                    reasons,
                    1
                )

                if registration_failure == (
                    "ConditionalCheckFailed"
                ):
                    logger.info(
                        "Duplicate registration rejected. "
                        "event_id=%s",
                        event_id
                    )

                    return build_response(
                        409,
                        {
                            "message": (
                                "This email address is already "
                                "registered for the event."
                            )
                        }
                    )

                if event_update_failure == (
                    "ConditionalCheckFailed"
                ):
                    if attempt < 2:
                        continue

                    latest_event = get_event(event_id)

                    if not latest_event:
                        return build_response(
                            404,
                            {
                                "message": (
                                    "The requested event was not found."
                                )
                            }
                        )

                    latest_capacity = int(
                        latest_event.get("capacity", 0)
                    )

                    latest_count = int(
                        latest_event.get(
                            "registered_count",
                            0
                        )
                    )

                    if latest_count >= latest_capacity:
                        return build_response(
                            409,
                            {"message": "The event is sold out."}
                        )

                    return build_response(
                        409,
                        {
                            "message": (
                                "The event was updated by another "
                                "request. Please retry registration."
                            )
                        }
                    )

                raise

        return build_response(
            409,
            {
                "message": (
                    "Registration could not be completed. "
                    "Please retry."
                )
            }
        )

    except ClientError:
        logger.exception(
            "DynamoDB registration operation failed."
        )

        return build_response(
            500,
            {"message": "An unexpected backend error occurred."}
        )

    except Exception:
        logger.exception(
            "Unexpected RegisterEventFunction failure."
        )

        return build_response(
            500,
            {"message": "An unexpected backend error occurred."}
        )