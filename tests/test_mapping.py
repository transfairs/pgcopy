from unittest.mock import patch

import pgcopy.config
from pgcopy.mapping import build_routing


@patch("pgcopy.mapping.get_secret_list")
@patch("pgcopy.mapping.format_secret")
def test_build_routing_builds_expected_structure(
    mock_format_secret, mock_get_list
):
    mock_get_list.return_value = {
        pgcopy.config.db_1: {"a": 1},
        pgcopy.config.db_2: {"b": 2},
    }

    mock_format_secret.return_value = (
        "host",
        5432,
        "user",
        "pwd",
        "db",
        "ssh",
    )

    routing = build_routing()

    assert isinstance(routing, dict)
    assert len(routing) == 1

    value = next(iter(routing.values()))
    assert value["db"] == f"{pgcopy.config.db_prefix}_2"
    assert "example_table" in value["tables"]
    assert "weather" in value["tables"]

    mock_get_list.assert_called_once()
    assert mock_format_secret.call_count >= 1
