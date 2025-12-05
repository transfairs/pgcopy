import pgcopy.config
from pgcopy.aws_secrets import get_secret
from pgcopy.connection import create_pg_connection
from pgcopy.mapping import build_routing
from pgcopy.routing import process_all_routes


def start() -> None:
    """
    Entry point for the data copy pipeline.

    Creates the source connection, builds the routing configuration and
    dispatches copy operations for all configured routes.
    """
    # Get source secrets
    (host, port, user, password, db_name, ssh_key) = get_secret(
        pgcopy.config.source
    )

    # Create connection via SSH tunnel
    conn, ssh_client = create_pg_connection(
        ssh_host=pgcopy.config.ssh_host,
        ssh_user="ec2-user",
        ssh_key=ssh_key,
        remote_host=host,
        db_name=db_name,
        db_password=password,
    )

    routing = build_routing()

    # Copy tables
    process_all_routes(
        conn, local_schema=pgcopy.config.source_schema, routing=routing
    )

    # Cleanup
    conn.close()
    ssh_client.close()
