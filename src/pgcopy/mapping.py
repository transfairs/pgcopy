import pgcopy.config
from pgcopy.aws_secrets import format_secret, get_secret_list  # , get_secret


def build_routing() -> dict[str, dict[str, int | str | list[str]]]:
    """
    Build the routing configuration based on Secrets Manager values.

    Secrets are retrieved on demand and transformed into a nested routing
    dictionary that is consumed by process_all_routes().
    """
    secrets_json = get_secret_list(
        f"{pgcopy.config.sm_prefix}{pgcopy.config.target_env}"
    )

    aws_db_1 = format_secret(secrets_json.get(pgcopy.config.db_1))
    aws_db_2 = format_secret(secrets_json.get(pgcopy.config.db_2))

    routing = {
        aws_db_1[0]: {
            "db": f"{pgcopy.config.db_prefix}_1",
            "password": aws_db_1[3],
            "tables": [
                "example_table",
                "traffic",
            ],
        },
        aws_db_2[0]: {
            "db": f"{pgcopy.config.db_prefix}_2",
            "password": aws_db_2[3],
            "tables": [
                "example_table",
                "customer",
                "orders",
                "weather",
            ],
        },
    }

    return routing
