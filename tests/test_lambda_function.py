import runpy
import sys
from unittest.mock import patch

from pgcopy.lambda_function import lambda_handler


@patch("pgcopy.lambda_function.start")
def test_lambda_handler_invokes_start_and_returns_ok(mock_start):
    event = {}
    context = object()

    resp = lambda_handler(event, context)

    mock_start.assert_called_once()
    assert resp["statusCode"] == 200
    assert '"OK"' in resp["body"]


def test_main_block_invokes_start_when_run_as_script():
    sys.modules.pop("pgcopy.lambda_function", None)
    with patch("pgcopy.main.start") as mock_start:
        runpy.run_module("pgcopy.lambda_function", run_name="__main__")

    mock_start.assert_called_once()
