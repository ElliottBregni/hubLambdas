
import copy
import datetime
import http
import json
import logging
import os
import uuid
import base64
import time
import multiprocessing
import sys

import fastjsonschema
from threading import Timer, Thread, Event
from multiprocessing import Process, Pool
from boto3 import client as boto3_client
from botocore.config import Config
from botocore.exceptions import ClientError
import phonenumbers as pn




REFERENCE_HEADER_NAME = "X-FV-Reference"


def add_reference_header(function):
    """
    Decorator for Lambda functions to add our tracing reference header.

    Now using the aws request id for the trace id
    """

    def decorator(event, context):
        trace_id = get_request_id(event)
        if "headers" not in event:
            event["headers"] = {}
        if not get_reference(event):
            event.setdefault("headers", {})[REFERENCE_HEADER_NAME] = trace_id
        return function(event, context)

    return decorator


def get_reference(event):
    """
    Returns our tracing reference from the event.
    """
    return get_header(event, REFERENCE_HEADER_NAME)


def get_request_id(event):
    trace_id = get_reference(event)
    if not trace_id:
        trace_id = event.get("requestContext", {}).get("requestId", "")
    return trace_id


def set_request_id(func):
    def set_id(event, context):
        trace_id = get_reference(event)
        if not trace_id:
            trace_id = event.get("requestContext", {}).get("requestId", "")
        set_header(event, REFERENCE_HEADER_NAME, trace_id)
        return func(event, context)

    return set_id


def get_event_body(event):
    """
    Extract body from Lambda event.

    Lambdas invoked via the API gateway receive the request payload as
    a JSON string in the body property of the event.

    When Lambdas are invoked directly via the Python SDK, the body
    property of the event contains whatever the invoker supplies,
    presumably a dict.

    So this function will try to parse a string body as JSON, or
    simply return a body that isn't a string.

    Args:
        event (dict): The Lambda event.

    Returns:
        dict: the parsed event body

    Raises:
        fv.error.ParameterError: if we had to do the parsing and couldn't.
    """
    if "body" not in event:
        raise fv.error.ParameterError("Event body not found.")

    if isinstance(event["body"], str):
        return get_parsed_event_body(event)

    return event["body"]


def get_parsed_event_body(event):
    """
    Extract and parse a string body from a Lambda event.
    """
    try:
        return json.loads(event.get("body", ""))
    except json.JSONDecodeError as e:
        logging.error("JSON decoding of event body failed: %s", e)
        raise fv.error.ParameterError(e) from e


def get_http_method(event):
    """
    Returns the HTTP method of the event, or None if not available.
    """
    return event.get("httpMethod")


# TODO: (tech debt) move this to the auth module
def get_privileges(event):
    try:
        return json.loads(event["requestContext"]["authorizer"]["privileges"])
    except KeyError as e:
        logging.error("Privileges could not be found on the event: %s", e)
        raise PermissionError("Error checking privileges") from e
    except json.JSONDecodeError as e:
        logging.error("JSON decoding of privileges failed: %s", e)
        raise PermissionError("Error checking privileges") from e


# TODO: (tech debt) move this to the auth module
def get_user_id(event):
    return event["requestContext"]["authorizer"]["user_id"].split("|")[1]


def get_header(event, name, required=False):
    """
    Gets the value of a header from a Lambda event.
    """

    if name in event.get("headers", {}):
        return event["headers"][name]

    lower_name = name.lower()
    for key in event.get("headers", {}).keys():
        if lower_name == key.lower():
            return event["headers"][key]

    if required:
        raise KeyError(name)

    return None


def get_multivalue_header(event, name, required=False):
    """
    Gets all values for a header from a Lambda event.
    """
    if name in event.get("multiValueHeaders", {}):
        return event["multiValueHeaders"][name]

    lower_name = name.lower()
    for key in event.get("multiValueHeaders", {}).keys():
        if lower_name == key.lower():
            return event["multiValueHeaders"][key]

    if required:
        raise KeyError(name)

    return None


def set_header(event, name, value):
    """
    Sets the value of a header in a Lambda event.
    """
    if "headers" in event:
        new_headers = copy.deepcopy(event["headers"])
        lower_name = name.lower()
        keys_to_delete = []
        for key in new_headers.keys():
            if lower_name == key.lower():
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del new_headers[key]

        new_headers[name] = value

        event["headers"] = new_headers


def set_multivalue_header(event, name, value=[]):
    """
    Sets the list of values for a header in a Lambda event.
    """
    new_headers = copy.deepcopy(event["multiValueHeaders"])
    lower_name = name.lower()
    keys_to_delete = []
    for key in new_headers.keys():
        if lower_name == key.lower():
            keys_to_delete.append(key)

    for key in keys_to_delete:
        del new_headers[key]

    new_headers[name] = value

    event["multiValueHeaders"] = new_headers


def get_path_parameter(event, name, required=False):
    pp = event.get("pathParameters") or {}
    pp = pp.get(name)
    if required and pp is None:
        raise KeyError(f"Missing path parameter: {name}")
    return pp


def get_query_parameter(event, name, required=False, default=None):

    qsp = event.get("queryStringParameters") or {}
    qp = qsp.get(name)
    if qp is None:
        if required is True:
            message = f"Missing query parameter: {name}"
            raise fv.error.BadRequestError(message, message)
        if default is not None:
            qp = default
    return qp


def get_boolean_query_parameter(event, name, required=False, default=None):
    qp = str(
        get_query_parameter(event, name, required, "t" if default is True else None)
    )
    b = qp and qp.lower() in ("1", "t", "y", "true")
    return b


def get_float_query_parameter(event, name, required=False, default=None):
    try:
        value = get_query_parameter(event, name, required, default)
        if value is None:
            if required is True:
                raise ValueError("Missing required float parameter")
            return None
        return float(value)
    except (KeyError, ValueError) as e:
        raise fv.error.ParameterError(f"Parameter {name} must be a decimal number")


def get_int_query_parameter(event, name, required=False, default=None):
    try:
        value = get_query_parameter(event, name, required, default)
        if value is None:
            if required:
                raise ValueError
            return None
        return int(value)
    except (KeyError, ValueError) as e:
        raise fv.error.ParameterError(f"Parameter {name} must be an integer")


def get_int_path_parameter(event, name, required=False):
    try:
        value = get_path_parameter(event, name, required)
        if value is None:
            if required:
                raise ValueError
            return None
        return int(value)
    except (KeyError, ValueError) as e:
        raise fv.error.ParameterError(f"Path parameter {name} must be an integer")


def get_int_list_query_parameter(event, name, required=False, default=None):
    try:
        return get_list_query_parameter(
            event, name, required, default, element_transformer=int
        )
    except fv.error.ParameterError as e:
        raise fv.error.ParameterError(
            f"Parameter {name} must be a list of comma-separated integers."
        ) from e


def get_list_query_parameter(
    event, name, required=False, default=None, element_transformer=str
):
    """
    Get the value of a query parameter intended to represent a list of values.

    The query parameter is first parsed as JSON, to support [thing1, thing2].

    If that fails or produces a string, we try to split the string on commas.

    If it produces a a scalar value that is not a string, that value
    will be wrapped in a list.

    Finally, the element_transformer function is applied to each value
    in the list, and the end result is returned as a tuple.
    """
    try:
        value = get_query_parameter(event, name, required, default)
        if value:
            values = value
            try:
                values = json.loads(value)
            except json.JSONDecodeError:
                pass

            if isinstance(values, str):
                if "," in values:
                    values = values.split(",")

            if not isinstance(values, list):
                values = [values]

            return tuple([element_transformer(v) for v in values])
        elif required:
            raise ValueError
        return None
    except (ValueError) as e:
        logging.error("Parsing of list query parameter failed: %s", e)
        raise fv.error.ParameterError(
            f"Parameter {name} must be a list of comma-separated values."
        ) from e


def get_timestamp_query_parameter(event, name, required=False, default=None):
    try:
        value = get_float_query_parameter(event, name, required, default)
        if value:
            return datetime.datetime.fromtimestamp(value).astimezone(
                datetime.timezone.utc
            )
        elif required:
            raise ValueError
        return None
    except (fv.error.ParameterError, OverflowError, OSError, ValueError) as e:
        raise fv.error.ParameterError(
            f"Could not parse timestamp in parameter {name}."
        ) from e


def make_response(body, status_code=200, content_type="application/json"):
    """
    Makes the response required for Lambda proxy integrations.

    If you're using a non-proxy integration between your Lambda
    function and the API gateway, this is not the full HTTP response
    you need to produce. See:

    https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-integration-settings-integration-response.html
    """
    body = (
        body
        if isinstance(body, str)
        else json.dumps(body, default=fv.encoding.json.encoder)
    )
    response = {
        "statusCode": str(status_code),
        "headers": {
            "Content-Type": str(content_type),
            "Access-Control-Allow-Origin": "*",
        },
        "body": body,
    }
    return response


def make_client_error(event, error_message):
    """
    Construct the minimal error structure we send to API clients.
    """
    return {"error": {"message": error_message, "reference": get_request_id(event)}}


def set_timeout(t):
    """
    Decorator to set a timeout on a function
    WARNING
    """

    def wrapper(func):
        def handle_timeout():
            pass

        def timer(event, context):
            try:
                with Pool(1) as pool:
                    r = pool.apply_async(func, (event, context))
                    result = r.get(timeout=2)
                    print(result)
            except multiprocessing.TimeoutError:
                print("Function timed out")
            # p.start()
            # timer = Timer(t, handle_timeout)
            # timer.start()
            # while True:
            #     time.sleep(1)
            #     if not timer.is_alive():
            #         p.terminate()
            #         print("process killed by timeout")
            #         break
            # p.join()

        return timer

    return wrapper


def redact_header(event, header_name, replacement="REDACTED"):
    """
    Replaces any value of the given header with the given string.
    """
    if get_header(event, header_name):
        set_header(event, header_name, replacement)

    if get_multivalue_header(event, header_name):
        set_multivalue_header(event, header_name, [replacement])

    return event


def sanitize(event):
    """
    Return a copy of the AWS Lambda event with sensitive information redacted.
    """

    new_event = copy.deepcopy(event)

    # authorization's a special case; we'd like to preserve the type
    authorization = get_header(new_event, "Authorization")
    authorizations = get_multivalue_header(new_event, "Authorization")
    if authorization or authorizations:
        auth_type, _ = authorization.split(maxsplit=1)
        redact_header(new_event, "Authorization", f"{auth_type} REDACTED")

    redact_header(new_event, "X-User-Token")

    return new_event


def log_event(event, message="event: %s"):
    """Logs a sanitized Lambda event.

    Message
    """
    if "%s" not in message:
        message += " %s"
    logging.info(
        message, json.dumps(sanitize(event), indent=2, default=fv.encoding.json.encoder)
    )


def make_error_response(
    event,
    context,
    message="Error",
    resource="",
    client_error_type="",
    client_message="",
    http_status=http.HTTPStatus.INTERNAL_SERVER_ERROR.value,
    service="shipment",
    error=None,
):
    """
    Create, log, and send the error to the appropriate topic to be consumed

    Args:
        event (dict): The Lambda event
        context (dict): The Lambda context
        message (str): The full message to be displayed in the logs
        resource (str): Deprecated
        client_error_type: Deprecated
        client_message (str): The message that's sent to the client
        request_id (str): The id that amazon creates for each request
        service (str): The overarching service that the error came from. i.e. shipments, entity. NOT ng_shipments

    Returns:
        dict: The error json that will be returned to the client
    """
    if error:
        message = getattr(error, "message", str(error))
        try:
            http_status = error.http_status
            client_message = error.client_message
        except AttributeError:
            # Unhandled exception
            http_status = http.HTTPStatus.INTERNAL_SERVER_ERROR.value
            client_message = "Unexpected error encountered"

    caller = context.function_name

    client_error = make_client_error(event, client_message)
    log_record = fv.log.make_log_record(
        message, caller, sanitize(event), client_error, http_status
    )

    logging.error(log_record)

    try:
        record_string = json.dumps(log_record)
        fv.aws.sns.send_exception(record_string, service)
    except Exception as e:
        logging.error("Could not log exception to SNS: %s", e)

    return make_response(client_error, http_status)


def request_audit(event, context, references={}):
    """
    Logs the receipt of a shipment event request.

    This builds a SNS message from the arguments and publishes it to
    the stage-specific received-shipment-requests topic.

    Args:
        references: The ids and other data points that we want to be searchable
    """
    logging.info(event)
    event = sanitize(event)
    receipt_log = {
        "request": context.function_name if context.function_name else "",
        "requestId": get_request_id(event),
        "body": json.dumps(event),
        "references": json.dumps(references),
    }
    aws_stage = os.environ["AWS_STAGE"]

    try:
        sns_client = boto3_client("sns")

        topic_name = f"{aws_stage}-received-requests"

        # Get the topic ARN by creating it if necessary.
        topic_arn = sns_client.create_topic(Name=topic_name)["TopicArn"]
        sns_client.publish(TopicArn=topic_arn, Message=json.dumps(receipt_log))
    except ClientError as ex:
        logging.error(f"Could not log received shipment: {ex}")


def create_filled_body(body, keyLst):
    return {key: body[key] if key in body else "" for key in keyLst}


def datetime_convert(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()


def invoke_lambda(
    name,
    headers=None,
    body=None,
    pathParameters=None,
    queryStringParameters=None,
    invoke_type="RequestResponse",
    full_payload=None,
    context={},
    timeout=300,
    retries=4,
):
    # Need to find out the stage we are running in
    # so we call the proper lambda in this stage
    aws_stage = os.environ["AWS_STAGE"]
    if full_payload:
        payload = full_payload
        if "headers" not in payload:
            payload["headers"] = {}
    else:
        payload = {}
        payload["body"] = body
        payload["headers"] = headers
        payload["pathParameters"] = pathParameters
        payload["queryStringParameters"] = queryStringParameters

    if context:
        context = json.dumps(context).encode("utf-8")
        context = base64.b64encode(context).decode("utf-8")

    config = Config(
        connect_timeout=timeout, read_timeout=timeout, retries={"max_attempts": retries}
    )

    # Invoke the function
    lambda_client = boto3_client("lambda", config=config)
    response = (
        lambda_client.invoke(
            FunctionName=f"{aws_stage}-{name}",
            InvocationType=invoke_type,
            Payload=json.dumps(payload, default=datetime_convert),
            ClientContext=json.dumps(context),
        )
        if context
        else lambda_client.invoke(
            FunctionName=f"{aws_stage}-{name}",
            InvocationType=invoke_type,
            Payload=json.dumps(payload, default=datetime_convert),
        )
    )
    if invoke_type == "RequestResponse":
        string_response = response["Payload"].read().decode("utf-8")
        parsed_response = json.loads(string_response)
        return parsed_response


def get_context_val(event, val):
    auth = event["requestContext"]["authorizer"]
    if val in ["organization_id", "privileges", "user_id", "email"]:
        return auth[val]
    return None


def get_url_extension(stage):
    # The dash is required because for some reason Auth0
    # is giving us back an access token with a client id
    # for test even if we're in a different enviro
    if stage == "prod":
        dash = ""
        search = ""
    elif stage == "prod-b":
        dash = ""
        search = "-b"
    elif stage == "test":
        dash = "-t"
        search = "-t"
    else:
        dash = "-s"
        search = "-s"
    return dash, search


def validate_phone_number(phone_number):
    country_codes = ["US", "CA", "MX"]
    for cc in country_codes:
        test_number = pn.parse(phone_number, cc)
        if pn.is_valid_number(test_number):
            return pn.format_number(test_number, pn.PhoneNumberFormat.E164)
    return None


def get_fvmb_phone_number(asset_id, expected_prefix="FVMB-", split_str="-"):
    """
    Returns the embedded phone number to be published to by SNS,
    if the asset assignment begins with the expected prefix, else None
    """
    phone_number = None
    if asset_id.startswith(expected_prefix):
        # The specified phone number follows a character that splits the string
        phone_number = asset_id.split(split_str)[1]
        phone_number = validate_phone_number(phone_number)
        if not phone_number:
            raise ValueError("The phone number provided is not valid")
    return phone_number


def get_fvmb_message():
    message = (
        "You have been assigned a shipment for delivery that can be confirmed by opening the following link. "
        + "Only access this link when you are in a parked position."
        + "\n\nhttps://itunes.apple.com/us/app/freightverify/id964528737?mt=8&app=itunes&ign-mpt=uo%3D4"
        + "\n\nhttps://play.google.com/store/apps/details?id=com.freightverify.fvmobile"
    )
    return message


def return_on_exception(service):
    def decorate(f):
        def applicator(event, context):
            try:
                return f(event, context)
            except Exception as e:
                return make_error_response(event, context, error=e, service=service)

        return applicator

    return decorate


def schema_validation(schema_validator):
    def decorator(f):
        def applicator(event, context):
            if schema_validator is not None:
                try:
                    jsonPayload = fv.aws.lambdas.get_event_body(event)
                    schema_validator(jsonPayload)
                except fastjsonschema.exceptions.JsonSchemaException as e:
                    trace_back = sys.exc_info()[2]
                    raise fv.error.BadRequestError(
                        message=str(e), client_message=str(e)
                    ).with_traceback(trace_back)
            return f(event, context)

        return applicator

    return decorator


def mandatory_lambda_handling(service="entity", privs=[], schema=None):
    """Decorator to call other decorators that are 'mandatory' on core lambdas"""

    def decorate(f):
        # # Sets the request id aka the trace id
        @fv.aws.lambdas.set_request_id
        @fv.log.config_logging
        # Decorator that wraps the function in a try catch and will return the proper
        @fv.aws.lambdas.return_on_exception(service)
        @fv.aws.lambdas.auth.check_privileges_dec(privs)
        @fv.aws.lambdas.schema_validation(schema)
        def applicator(event, context):
            event["body"] = fv.aws.lambdas.get_event_body(event)
            return f(event, context)

        return applicator

    return decorate


# H1-339


def get_fvmb_message_v2(shipment):
    message = (
        "You have been assigned the following shipment, that will need to be tracked by mobile phone."
        "\n\nPickup:"
        + f"\n{shipment.origin_name}"
        + f"\n{shipment.origin_address}  {shipment.origin_city}, {shipment.origin_state} {shipment.origin_postal_code}"
        + f"\n{shipment.origin_earliest_arrival}"
        + f"\n{shipment.origin_latest_arrival}"
        + "\n\nDelivery:"
        + f"\n{shipment.destination_name}"
        + f"\n{shipment.destination_address}  {shipment.destination_city}, {shipment.destination_state} {shipment.destination_postal_code}"
        + f"\n{shipment.destination_earliest_arrival}"
        + f"\n{shipment.destination_latest_arrival}"
        + "\n\n"
        + "Please start the FreightVerify mobile tracking application during pickup to begin tracking this shipment."
        + "\nIf you do not have the FreightVerify mobile tracking application, it can be downloaded from the Apple"
        + "\nApp Store or Google Play."
    )
    return message
