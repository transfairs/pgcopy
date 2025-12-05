import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from pgcopy.fdw_copy import (
    _create_server_object,
    _drop_server_object,
    _get_local_rows,
    _get_remote_cols_and_types,
    _literal,
    copy_local_to_remote_via_dblink_values,
)


def test_literal_none_returns_NULL():
    assert _literal(None) == "NULL"


def test_literal_simple_string_is_quoted():
    out = _literal("hello")
    assert out == "'hello'"


def test_literal_json_dict_when_rtype_jsonb():
    v = {"a": 1, "b": "x"}
    out = _literal(v, rtype="jsonb")
    assert out.startswith("'")
    assert out.endswith("'")
    inner = out[1:-1]
    data = json.loads(inner)
    assert data == v


def test_literal_list_to_text_array():
    v = ["A", "B"]
    out = _literal(v, rtype="text[]")
    assert out.startswith("'")
    assert out.endswith("'")
    assert "{A,B}" in out


def test_literal_list_with_none_to_text_array():
    v = ["A", None, "B"]
    out = _literal(v, rtype="text[]")
    assert out.startswith("'")
    assert out.endswith("'")
    inner = out[1:-1]
    assert "{A,NULL,B}" in inner


def test_literal_bytes_decoding_uses_fallback_codecs():
    raw = "ümlaut".encode("latin-1")
    out = _literal(raw)
    assert out.startswith("'")
    assert out.endswith("'")
    inner = out[1:-1]
    assert "ümlaut" in inner


@patch("pgcopy.fdw_copy.adapt")
def test_literal_falls_back_when_adapt_raises(mock_adapt):
    mock_adapt.side_effect = Exception("boom")

    out = _literal("O'Reilly")
    assert out == "'O''Reilly'"


def _make_conn_with_local_columns(local_cols):
    """
    Create a fake psycopg2-like connection whose cursor() context manager
    returns a cursor with an execute() that serves information_schema.columns.
    """
    conn = MagicMock()
    cur = MagicMock()

    conn.cursor.return_value.__enter__.return_value = cur

    def execute_side_effect(query, params=None):
        text = str(query)
        if "information_schema.columns" in text:
            cur.fetchall.return_value = [(c,) for c in local_cols]

    cur.execute.side_effect = execute_side_effect
    return conn


@patch("pgcopy.fdw_copy._create_server_object")
@patch("pgcopy.fdw_copy._get_local_rows")
@patch("pgcopy.fdw_copy._get_remote_cols_and_types")
def test_copy_raises_if_no_overlapping_columns(
    mock_get_remote_cols,
    mock_get_local_rows,
    mock_create_server_object,
):
    """
    If there are no overlapping column names between local and remote tables,
    the function must raise ValueError.
    """
    # Remote has only "remote_only"
    mock_get_remote_cols.return_value = [("remote_only", "text")]

    # Local has only "local_only"
    conn = _make_conn_with_local_columns(local_cols=["local_only"])

    # No rows should be fetched when there is no overlap
    mock_get_local_rows.return_value = []

    with pytest.raises(ValueError, match="No overlapping columns"):
        copy_local_to_remote_via_dblink_values(
            conn=conn,
            local_schema="public",
            local_table="local_table",
            remote_host="remote-host",
            remote_schema="public",
            remote_table="remote_table",
            remote_db="remote_db",
            remote_password="pwd",
            remote_user="postgres",
            remote_server="remote_srv",
            remote_port=5432,
            batch_size=1000,
            row_limit=None,
        )

    mock_create_server_object.assert_called_once()
    mock_get_local_rows.assert_not_called()


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
@patch("pgcopy.fdw_copy._create_server_object")
@patch("pgcopy.fdw_copy._get_local_rows")
@patch("pgcopy.fdw_copy._get_remote_cols_and_types")
def test_copy_success_with_simple_overlap(
    mock_get_remote_cols,
    mock_get_local_rows,
    mock_create_server_object,
    mock_as_string,
):
    """
    Happy-path test: remote and local share the columns (id, name),
    and rows are copied successfully. We only verify that the function
    returns True and does not raise, not the exact SQL text.
    """
    # Remote has id and name with simple types
    mock_get_remote_cols.return_value = [
        ("id", "integer"),
        ("name", "text"),
    ]

    # Local side has at least these columns
    conn = _make_conn_with_local_columns(local_cols=["id", "name", "ignore"])

    mock_get_local_rows.return_value = [
        (1, "Alice"),
        (2, "Bob"),
    ]

    result = copy_local_to_remote_via_dblink_values(
        conn=conn,
        local_schema="public",
        local_table="local_table",
        remote_host="remote-host",
        remote_schema="public",
        remote_table="remote_table",
        remote_db="remote_db",
        remote_password="pwd",
        remote_user="postgres",
        remote_server="remote_srv",
        remote_port=5432,
        batch_size=1000,
        row_limit=None,
    )

    assert result is True

    mock_as_string.assert_called()
    mock_create_server_object.assert_called_once()
    mock_get_local_rows.assert_called_once()


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
@patch("pgcopy.fdw_copy._create_server_object")
@patch("pgcopy.fdw_copy._get_local_rows")
@patch("pgcopy.fdw_copy._get_remote_cols_and_types")
def test_copy_returns_none_when_no_rows(
    mock_get_remote_cols,
    mock_get_local_rows,
    mock_create_server_object,
    mock_as_string,
    caplog,
):
    mock_get_remote_cols.return_value = [("id", "integer")]

    conn = _make_conn_with_local_columns(local_cols=["id"])

    mock_get_local_rows.return_value = []

    with caplog.at_level(logging.WARN):
        result = copy_local_to_remote_via_dblink_values(
            conn=conn,
            local_schema="public",
            local_table="local_table",
            remote_host="remote-host",
            remote_schema="public",
            remote_table="remote_table",
            remote_db="remote_db",
            remote_password="pwd",
            remote_user="postgres",
            remote_server="remote_srv",
            remote_port=5432,
            batch_size=1000,
            row_limit=None,
        )

    assert not result
    assert any("No rows to copy" in r.message for r in caplog.records)


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
@patch("pgcopy.fdw_copy._create_server_object")
@patch("pgcopy.fdw_copy._get_local_rows")
@patch("pgcopy.fdw_copy._get_remote_cols_and_types")
def test_copy_logs_and_rolls_back_on_dblink_error(
    mock_get_remote_cols,
    mock_get_local_rows,
    mock_create_server_object,
    mock_as_string,
    caplog,
):
    mock_get_remote_cols.return_value = [
        ("id", "integer"),
        ("name", "text"),
    ]

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    def execute_side_effect(query, params=None):
        text = str(query)
        if "information_schema.columns" in text:
            cur.fetchall.return_value = [("id",)]
        elif "dblink_exec" in text:
            raise RuntimeError("dblink failure")

    cur.execute.side_effect = execute_side_effect

    mock_get_local_rows.return_value = [
        (None,),
    ]

    with caplog.at_level(logging.ERROR):
        result = copy_local_to_remote_via_dblink_values(
            conn=conn,
            local_schema="public",
            local_table="local_table",
            remote_host="remote-host",
            remote_schema="public",
            remote_table="remote_table",
            remote_db="remote_db",
            remote_password="pwd",
            remote_user="postgres",
            remote_server="remote_srv",
            remote_port=5432,
            batch_size=1000,
            row_limit=None,
        )

    assert result is False
    assert any("Chunk 1 failed" in r.message for r in caplog.records)

    mock_as_string.assert_called()


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
@patch("pgcopy.fdw_copy._create_server_object")
@patch("pgcopy.fdw_copy._get_local_rows")
@patch("pgcopy.fdw_copy._get_remote_cols_and_types")
def test_copy_logs_chunk_insert_when_more_rows_than_batch(
    mock_get_remote_cols,
    mock_get_local_rows,
    mock_create_server_object,
    mock_as_string,
    caplog,
):
    mock_get_remote_cols.return_value = [
        ("id", "integer"),
    ]

    conn = _make_conn_with_local_columns(local_cols=["id"])

    mock_get_local_rows.return_value = [
        (1,),
        (2,),
    ]

    with caplog.at_level(logging.INFO):
        result = copy_local_to_remote_via_dblink_values(
            conn=conn,
            local_schema="public",
            local_table="local_table",
            remote_host="remote-host",
            remote_schema="public",
            remote_table="remote_table",
            remote_db="remote_db",
            remote_password="pwd",
            remote_user="postgres",
            remote_server="remote_srv",
            remote_port=5432,
            batch_size=1,
            row_limit=None,
        )

    assert result is True
    assert any(
        "Chunk" in r.message and "inserted" in r.message
        for r in caplog.records
    )

    mock_as_string.assert_called()


def _make_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
def test_get_remote_cols_and_types_returns_rows(
    mock_as_string,
):
    conn, cur = _make_conn()
    cur.fetchall.return_value = [("id", "integer"), ("name", "text")]

    rows = _get_remote_cols_and_types(conn, "srv", "public", "tbl")

    assert rows == [("id", "integer"), ("name", "text")]
    cur.execute.assert_called_once()
    conn.cursor.return_value.__exit__.assert_called_once()

    mock_as_string.assert_called()


@patch("pgcopy.fdw_copy.sql.Composed.as_string", return_value="INSERT DUMMY")
def test_get_remote_cols_and_types_raises_if_no_rows(
    mock_as_string,
):
    conn, cur = _make_conn()
    cur.fetchall.return_value = []

    with pytest.raises(ValueError):
        _get_remote_cols_and_types(conn, "srv", "public", "tbl")

    mock_as_string.assert_called()


def test_get_local_rows_with_and_without_limit():
    conn, cur = _make_conn()
    cur.fetchall.return_value = [(1, "Alice")]

    rows = _get_local_rows(conn, "public", "tbl", ["id", "name"], limit=None)
    assert rows == [(1, "Alice")]
    assert cur.execute.call_count == 1

    cur.execute.reset_mock()
    rows = _get_local_rows(conn, "public", "tbl", ["id"], limit=10)
    assert rows == [(1, "Alice")]
    assert "LIMIT" in str(cur.execute.call_args[0][0])


def test_create_server_object_builds_server_and_user_mapping():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    with patch("pgcopy.fdw_copy._drop_server_object") as mock_drop:
        _create_server_object(
            conn=conn,
            remote_host="host",
            remote_db="db",
            remote_password="pwd",
            remote_server="srv",
            remote_user="user",
            remote_port=5432,
        )

    cur.execute.assert_any_call("CREATE EXTENSION IF NOT EXISTS dblink;")

    sql_texts = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert any("CREATE SERVER IF NOT EXISTS srv" in s for s in sql_texts)

    assert any(
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER" in s
        for s in sql_texts
    )

    assert any(
        "SELECT srvname, srvoptions FROM pg_foreign_server" in s
        for s in sql_texts
    )

    mock_drop.assert_called_once_with(conn, "srv")

    conn.commit.assert_called_once()


def test_drop_server_object_with_cascade_flag():
    conn, cur = _make_conn()

    _drop_server_object(conn, "srv", cascade=True)

    cur.execute.assert_called_once()
    # SQL enthält 'DROP SERVER IF EXISTS' und 'CASCADE'
    sql_text = str(cur.execute.call_args[0][0])
    assert "DROP SERVER IF EXISTS" in sql_text
    assert "CASCADE" in sql_text
    conn.commit.assert_called_once()
