import json
import logging
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

EVENTS_TABLE = os.environ["EVENTS_TABLE"]

dynamodb = boto3.resource("dynamodb")
events_table = dynamodb.Table(EVENTS_TABLE)


def decimal_to_number(value):
    """Convert DynamoDB Decimal values into JSON-compatible numbers."""
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)

        return float(value)

    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

from decimal import Decimal


def decimal_default(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)

    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )
def build_response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
        },
        "body": json.dumps(payload, default=decimal_default)
    }


def retrieve_all_events() -> list:
    """Scan the Events table and follow pagination when required."""
    events = []
    scan_arguments = {}

    while True:
        response = events_table.scan(**scan_arguments)
        events.extend(response.get("Items", []))

        last_key = response.get("LastEvaluatedKey")

        if not last_key:
            break

        scan_arguments["ExclusiveStartKey"] = last_key

    return events


def lambda_handler(event, context):
    try:
        events = retrieve_all_events()

        events.sort(
            key=lambda item: (
                item.get("event_date", ""),
                item.get("event_name", "")
            )
        )

        logger.info("Retrieved %d event records.", len(events))

        return build_response(
            200,
            {
                "message": "Events retrieved successfully.",
                "count": len(events),
                "events": events
            }
        )

    except ClientError:
        logger.exception("DynamoDB event retrieval failed.")

        return build_response(
            500,
            {
                "message": "An unexpected backend error occurred."
            }
        )

    except Exception:
        logger.exception("Unexpected ListEventsFunction failure.")

        return build_response(
            500,
            {
                "message": "An unexpected backend error occurred."
            }
        )