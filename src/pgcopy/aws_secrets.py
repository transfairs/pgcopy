import json
from typing import Any, Dict, Optional, Tuple, cast

import boto3
from botocore.exceptions import ClientError

import pgcopy.config

"""
Retrieves a secret from AWS Secrets Manager and converts it into a structured
tuple for downstream use.
"""

DEFAULT_REGION = f"{pgcopy.config.region}"


def _ensure_dict(
    v: Optional[dict[str, Optional[Any]]],
) -> dict[str, Optional[Any]]:
    if v is None:
        raise AttributeError("Expected dict[str, Optional[Any]], got None")
    return v


def _retrieve_from_aws(
    secret_name: str, region_name: str
) -> Dict[str, Optional[Dict[str, Optional[Any]]]]:
    """
    Calls AWS Secrets Manager to fetch the raw secret JSON for the given
    secret name and region, returning it as a decoded Python object.
    """
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    return cast(Dict[str, Optional[Dict[str, Optional[Any]]]], secret)


def get_secret(
    secret_name: str, region_name: str = DEFAULT_REGION
) -> Tuple[Any, ...]:
    """
    Retrieves the secret and maps it into a tuple containing host, port,
    username, password, database instance identifier, and SSH metadata.
    """

    secret_data = _retrieve_from_aws(secret_name, region_name)

    return format_secret(secret_data)


def get_secret_list(
    secret_name: str, region_name: str = DEFAULT_REGION
) -> Dict[str, Optional[Dict[str, Optional[Any]]]]:
    """
    Convenience wrapper returning the decoded JSON object exactly as stored
    in Secrets Manager.
    """
    return _retrieve_from_aws(secret_name, region_name)


def format_secret(
    secret: Optional[Dict[str, Optional[Any]]],
) -> Tuple[Any, ...]:
    """
    Extracts selected fields from the secret JSON and returns them in a
    fixed-order tuple.
    """
    return (
        _ensure_dict(secret).get("host"),
        _ensure_dict(secret).get("port"),
        _ensure_dict(secret).get("username"),
        _ensure_dict(secret).get("password"),
        _ensure_dict(secret).get("dbInstanceIdentifier"),
        _ensure_dict(secret).get("ssh"),
    )
