import json
import logging
import os
import re
import uuid
from datetime import date

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

EVENTS_TABLE = os.environ["EVENTS_TABLE"]

dynamodb = boto3.resource("dynamodb")
events_table = dynamodb.Table(EVENTS_TABLE)


def build_response(status_code, payload):
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
    """Parse requests from API Gateway or direct Lambda tests."""
    body = event.get("body", event)

    if isinstance(body, str):
        return json.loads(body)

    if isinstance(body, dict):
        return body

    raise ValueError("Request body must be a JSON object.")


def valid_iso_date(value: str) -> bool:
    """Validate a date written as YYYY-MM-DD."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return False

    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def lambda_handler(event, context):
    try:
        if not isinstance(event, dict):
            return build_response(
                400,
                {"message": "Invalid request format."}
            )

        try:
            request_body = parse_request_body(event)
        except (json.JSONDecodeError, TypeError, ValueError):
            return build_response(
                400,
                {"message": "Request body must contain valid JSON."}
            )

        required_fields = [
            "event_name",
            "event_date",
            "location",
            "capacity"
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in request_body
            or request_body[field] is None
            or (
                isinstance(request_body[field], str)
                and not request_body[field].strip()
            )
        ]

        if missing_fields:
            return build_response(
                400,
                {
                    "message": "Required fields are missing.",
                    "missing_fields": missing_fields
                }
            )

        event_name = request_body["event_name"]
        event_date = request_body["event_date"]
        location = request_body["location"]
        capacity = request_body["capacity"]

        if not isinstance(event_name, str) or not event_name.strip():
            return build_response(
                400,
                {"message": "event_name must be a non-empty string."}
            )

        if not isinstance(location, str) or not location.strip():
            return build_response(
                400,
                {"message": "location must be a non-empty string."}
            )

        if not isinstance(event_date, str) or not valid_iso_date(event_date):
            return build_response(
                400,
                {"message": "event_date must use the YYYY-MM-DD format."}
            )

        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or capacity <= 0
        ):
            return build_response(
                400,
                {"message": "capacity must be a positive whole number."}
            )

        event_id = f"EVT-{uuid.uuid4().hex[:8].upper()}"

        event_item = {
            "event_id": event_id,
            "event_name": event_name.strip(),
            "event_date": event_date,
            "location": location.strip(),
            "capacity": capacity,
            "registered_count": 0,
            "status": "AVAILABLE"
        }

        events_table.put_item(
            Item=event_item,
            ConditionExpression="attribute_not_exists(event_id)"
        )

        logger.info(
            "Event created successfully. event_id=%s",
            event_id
        )

        return build_response(
            201,
            {
                "message": "Event created successfully.",
                "event": event_item
            }
        )

    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")

        if error_code == "ConditionalCheckFailedException":
            logger.warning("Event identifier conflict detected.")

            return build_response(
                409,
                {"message": "An event with this identifier already exists."}
            )

        logger.exception("DynamoDB operation failed.")

        return build_response(
            500,
            {"message": "An unexpected backend error occurred."}
        )

    except Exception:
        logger.exception("Unexpected CreateEventFunction failure.")

        return build_response(
            500,
            {"message": "An unexpected backend error occurred."}
        )