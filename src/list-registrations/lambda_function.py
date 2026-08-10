import json
import logging
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGISTRATIONS_TABLE = os.environ["REGISTRATIONS_TABLE"]

dynamodb = boto3.resource("dynamodb")
registrations_table = dynamodb.Table(REGISTRATIONS_TABLE)


def decimal_to_number(value):
    """Convert DynamoDB Decimal values into JSON-compatible numbers."""
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
        "body": json.dumps(payload)
    }


def extract_event_id(event: dict) -> str:
    """Read and clean event_id from API Gateway path parameters."""
    path_parameters = event.get("pathParameters") or {}
    event_id = path_parameters.get("event_id")

    if not isinstance(event_id, str):
        return ""

    return event_id.strip()


def retrieve_registrations(event_id: str) -> list:
    """Query all registrations for an event and handle pagination."""
    registrations = []

    query_arguments = {
        "KeyConditionExpression": Key("event_id").eq(event_id),
        "ConsistentRead": True
    }

    while True:
        response = registrations_table.query(**query_arguments)

        registrations.extend(
            response.get("Items", [])
        )

        last_key = response.get("LastEvaluatedKey")

        if not last_key:
            break

        query_arguments["ExclusiveStartKey"] = last_key

    return registrations


def lambda_handler(event, context):
    try:
        if not isinstance(event, dict):
            return build_response(
                400,
                {
                    "message": "Invalid request format."
                }
            )

        event_id = extract_event_id(event)

        if not event_id:
            return build_response(
                400,
                {
                    "message": (
                        "event_id is required in pathParameters."
                    )
                }
            )

        registrations = retrieve_registrations(event_id)

        registrations.sort(
            key=lambda item: (
                item.get("registered_at", ""),
                item.get("email", "")
            )
        )

        logger.info(
            "Retrieved %d registration records. event_id=%s",
            len(registrations),
            event_id
        )

        return build_response(
            200,
            {
                "message": (
                    "Registrations retrieved successfully."
                ),
                "event_id": event_id,
                "count": len(registrations),
                "registrations": registrations
            }
        )

    except ClientError:
        logger.exception(
            "DynamoDB registration retrieval failed."
        )

        return build_response(
            500,
            {
                "message": (
                    "An unexpected backend error occurred."
                )
            }
        )

    except Exception:
        logger.exception(
            "Unexpected ListRegistrationsFunction failure."
        )

        return build_response(
            500,
            {
                "message": (
                    "An unexpected backend error occurred."
                )
            }
        )