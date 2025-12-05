import json
from typing import Any, Dict

from pgcopy.main import start


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point that triggers the data copy pipeline."""
    start()
    return {"statusCode": 200, "body": json.dumps("OK")}


if __name__ == "__main__":
    start()
