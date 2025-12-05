import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path

import psycopg2

from pgcopy.fdw_copy import copy_local_to_remote_via_dblink_values

log_dir = Path("log")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            f"log/audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)

DEFAULT_REMOTE_SERVER_PREFIX = "pygrate_"


def _force_str(v: int | str | list[str]) -> str:
    return str(v) if isinstance(v, int | str) else v[0]


def _force_list(v: int | str | list[str]) -> list[str]:
    return v if isinstance(v, list) else [str(v)]


def _ensure_int(v: int | str | list[str]) -> int:
    if isinstance(v, int):
        return v
    raise TypeError(f"Expected int, got {type(v)}")


def process_all_routes(
    conn: psycopg2.extensions.connection,
    local_schema: str,
    routing: dict[str, dict[str, int | str | list[str]]],
    remote_server_prefix: str = DEFAULT_REMOTE_SERVER_PREFIX,
) -> None:
    """
    Executes bulk copy operations for all configured remote hosts.

    For every host in the routing configuration, this function iterates over
    all listed tables and invokes `copy_local_to_remote_via_dblink_values` to
    copy data from the local schema into the corresponding remote schema via
    dblink.
    """
    for host, cfg in routing.items():
        dbname = cfg["db"]
        pwd = cfg["password"]
        tables: list[str] = _force_list(cfg["tables"])
        remote_port = cfg.get("port", 5432)
        remote_schema = cfg.get("schema", "public")

        remote_server = f"{remote_server_prefix}{dbname}".replace("-", "_")
        logging.info(
            f"\n=== Processing remote {dbname}@{host} "
            f"({len(tables)} table(s)) ==="
        )

        for local_table in tables:
            remote_table = local_table
            logging.info(
                f"→ Copying {local_schema}.{local_table} → {dbname}."
                f"{remote_table}"
            )
            try:
                done = copy_local_to_remote_via_dblink_values(
                    conn=conn,
                    local_schema=local_schema,
                    local_table=local_table,
                    remote_schema=_force_str(remote_schema),
                    remote_table=remote_table,
                    remote_host=host,
                    remote_port=_ensure_int(remote_port),
                    remote_password=_force_str(pwd),
                    remote_db=_force_str(dbname),
                    remote_server=remote_server,
                )
                icon = "✅" if done else "⚠️"
                logging.info(f"{icon} Done: {local_table}")
            except Exception as e:
                logging.info(
                    f"❌ Error copying {local_table} "
                    f"({e.__class__.__name__}): {e}"
                )
                traceback.print_exc()
