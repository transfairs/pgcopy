import json
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

import pgcopy.config
from pgcopy.aws_secrets import (
    _ensure_dict,
    _retrieve_from_aws,
    format_secret,
    get_secret,
    get_secret_list,
)


def test_format_secret_extracts_expected_tuple():
    data = {
        "host": "h",
        "port": 5432,
        "username": "u",
        "password": "p",
        "dbInstanceIdentifier": "db",
        "ssh": "ssh-key",
    }
    result = format_secret(data)
    assert result == ("h", 5432, "u", "p", "db", "ssh-key")


@patch("pgcopy.aws_secrets._retrieve_from_aws")
def test_get_secret_uses_retrieve_and_formats(mock_retrieve):
    mock_retrieve.return_value = {
        "host": "h",
        "port": 5432,
        "username": "u",
        "password": "p",
        "dbInstanceIdentifier": "db",
        "ssh": "ssh-key",
    }
    host, port, user, password, db_name, ssh = get_secret(
        "name", region_name=f"{pgcopy.config.region}"
    )
    assert host == "h"
    assert db_name == "db"
    assert ssh == "ssh-key"
    mock_retrieve.assert_called_once_with("name", f"{pgcopy.config.region}")


@patch("pgcopy.aws_secrets._retrieve_from_aws")
def test_get_secret_list_returns_raw(mock_retrieve):
    mock_retrieve.return_value = {"k": "v"}
    result = get_secret_list("name")
    assert result == {"k": "v"}


@patch("pgcopy.aws_secrets.boto3.session.Session")
def test_retrieve_from_aws_success(mock_session_cls):
    mock_session = mock_session_cls.return_value
    mock_client = mock_session.client.return_value

    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"host": "h"})
    }

    result = _retrieve_from_aws("name", region_name=f"{pgcopy.config.region}")

    mock_session.client.assert_called_once_with(
        service_name="secretsmanager",
        region_name=f"{pgcopy.config.region}",
    )
    mock_client.get_secret_value.assert_called_once_with(SecretId="name")
    assert result == {"host": "h"}


@patch("pgcopy.aws_secrets.boto3.session.Session")
def test_retrieve_from_aws_raises_client_error(mock_session_cls):
    mock_session = mock_session_cls.return_value
    mock_client = mock_session.client.return_value

    error = ClientError(
        error_response={"Error": {"Code": "ResourceNotFoundException"}},
        operation_name="GetSecretValue",
    )
    mock_client.get_secret_value.side_effect = error

    with pytest.raises(ClientError):
        _retrieve_from_aws("name", region_name=f"{pgcopy.config.region}")


def test_ensure_dict_raises_on_none():
    with pytest.raises(AttributeError):
        _ensure_dict(None)
