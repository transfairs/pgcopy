from unittest.mock import MagicMock, patch

from pytest import raises

from pgcopy.routing import _ensure_int, process_all_routes


def test_process_all_routes_calls_copy_for_each_table():
    conn = MagicMock()

    routing = {
        "host1": {
            "db": "db1",
            "password": "pwd1",
            "tables": ["t1", "t2"],
        },
        "host2": {
            "db": "db2",
            "password": "pwd2",
            "tables": ["t3"],
            "schema": "custom_schema",
            "port": 5555,
        },
    }

    with patch(
        "pgcopy.routing.copy_local_to_remote_via_dblink_values",
        return_value=True,
    ) as mock_copy:
        process_all_routes(
            conn,
            local_schema="pipeline",
            routing=routing,
            remote_server_prefix="pygrate_",
        )

    assert mock_copy.call_count == 3

    first_call_kwargs = mock_copy.call_args_list[0].kwargs
    assert first_call_kwargs["local_schema"] == "pipeline"
    assert first_call_kwargs["remote_db"] == "db1"
    assert first_call_kwargs["remote_host"] == "host1"


def _minimal_routing():
    return {
        "ExampleDatabase": {
            "host": "h",
            "port": 5432,
            "user": "u",
            "password": "p",
            "db": "db",
            "remote_server": "srv",
            "tables": ["tbl1"],
        }
    }


@patch("pgcopy.routing.copy_local_to_remote_via_dblink_values")
def test_process_all_routes_success(mock_copy):
    mock_copy.return_value = True
    conn = MagicMock()

    process_all_routes(conn, local_schema="public", routing=_minimal_routing())

    mock_copy.assert_called_once()


@patch("pgcopy.routing.traceback.print_exc")
@patch("pgcopy.routing.copy_local_to_remote_via_dblink_values")
def test_process_all_routes_logs_error_on_exception(mock_copy, mock_print_exc):
    mock_copy.side_effect = RuntimeError("boom")
    conn = MagicMock()

    process_all_routes(conn, local_schema="public", routing=_minimal_routing())

    mock_copy.assert_called_once()
    mock_print_exc.assert_called_once()


def test_ensure_int_raises_on_str():
    with raises(TypeError):
        _ensure_int("abc")


def test_ensure_int_raises_on_list():
    with raises(TypeError):
        _ensure_int(["1", "2"])
