from unittest.mock import MagicMock, patch

import pgcopy.config
from pgcopy.main import start


@patch("pgcopy.main.process_all_routes")
@patch("pgcopy.main.build_routing")
@patch("pgcopy.main.create_pg_connection")
@patch("pgcopy.main.get_secret")
def test_start_wires_everything_and_closes(
    mock_get_secret,
    mock_create_pg_conn,
    mock_build_routing,
    mock_process_all_routes,
):
    mock_get_secret.return_value = (
        "ssh-host",
        22,
        "user_ignored",
        "pwd",
        "db",
        "PRIVATE_KEY",
    )

    conn = MagicMock()
    ssh_client = MagicMock()
    mock_create_pg_conn.return_value = (conn, ssh_client)

    routing = {"dummy": "routing"}
    mock_build_routing.return_value = routing

    start()

    mock_get_secret.assert_called_once()

    mock_create_pg_conn.assert_called_once()
    _, kwargs = mock_create_pg_conn.call_args

    assert kwargs["ssh_host"] == pgcopy.config.ssh_host
    assert kwargs["ssh_user"] == pgcopy.config.ssh_user

    assert kwargs["ssh_key"] == "PRIVATE_KEY"
    assert kwargs["remote_host"] == "ssh-host"
    assert kwargs["db_name"] == "db"
    assert kwargs["db_password"] == "pwd"

    mock_build_routing.assert_called_once()
    mock_process_all_routes.assert_called_once_with(
        conn, local_schema=pgcopy.config.source_schema, routing=routing
    )

    conn.close.assert_called_once()
    ssh_client.close.assert_called_once()
