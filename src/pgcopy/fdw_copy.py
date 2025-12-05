import json
import logging
from typing import Any, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import adapt

DEFAULT_USER = "postgres"
DEFAULT_PORT = 5432
DEFAULT_BATCH_SIZE = 1000
DEFAULT_SERVER = "localhost"
CASCADE = True


def _get_remote_cols_and_types(
    conn: psycopg2.extensions.connection,
    remote_server: str,
    remote_schema: str,
    remote_table: str,
) -> List[Tuple[str, str]]:
    """
    Returns a list of (column_name, column_type) pairs for a remote table
    by querying it through dblink.
    """
    with conn.cursor() as cur:
        # Define a remote query that returns column name and exact SQL type
        # text (format_type)
        remote_q = sql.SQL(
            """
            SELECT a.attname AS column_name,
                   format_type(a.atttypid, a.atttypmod) AS column_type
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {schema}
              AND c.relname  = {table}
              AND a.attgenerated = ''
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """
        ).format(
            schema=sql.Literal(remote_schema), table=sql.Literal(remote_table)
        )

        remote_q_str = remote_q.as_string(conn)

        # Call dblink to run the query on the remote server
        cur.execute(
            sql.SQL(
                """
                SELECT * FROM dblink(%s, %s)
                AS t(column_name text, column_type text);
            """
            ),
            (remote_server, remote_q_str),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(
                f"Remote table {remote_schema}.{remote_table} \
                             not found or has no columns."
            )
        return rows  # list of (name, type_text)


def _get_local_rows(
    conn: psycopg2.extensions.connection,
    local_schema: str,
    local_table: str,
    col_names: Sequence[str],
    limit: Optional[int],
) -> List[Tuple[Any, ...]]:
    """
    Retrieves local rows for the given set of column names.
    """
    with conn.cursor() as cur:
        cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in col_names)
        q = sql.SQL("SELECT {cols} FROM {sch}.{tbl}").format(
            cols=cols_sql,
            sch=sql.Identifier(local_schema),
            tbl=sql.Identifier(local_table),
        )
        if limit is not None:
            q = q + sql.SQL(" LIMIT {}").format(sql.Literal(limit))
        cur.execute(q)
        return cur.fetchall()


def _literal(v: Any, rtype: Optional[str] = None) -> str:
    if v is None:
        return "NULL"

    if isinstance(v, list) and rtype and "[]" in rtype:
        escaped_items = []
        for x in v:
            if x is None:
                escaped_items.append("NULL")
            else:
                escaped_items.append(
                    str(x).replace('"', '\\"').replace("'", "''")
                )
        return "'{" + ",".join(escaped_items) + "}'"

    if isinstance(v, (dict, list)) or (
        rtype and rtype.lower() in ("json", "jsonb")
    ):
        v = json.dumps(v, ensure_ascii=False)

    codecs = ("utf-8", "latin-1", "windows-1254", "iso-8859-9")
    for enc in codecs:
        if isinstance(v, bytes):
            try:
                v = v.decode(enc)
                break
            except (UnicodeDecodeError, LookupError):
                continue

    try:
        return str(adapt(v).getquoted().decode("utf-8"))
    except Exception:
        return str("'" + v.replace("'", "''") + "'")


def copy_local_to_remote_via_dblink_values(
    conn: psycopg2.extensions.connection,
    local_schema: str,
    local_table: str,
    remote_host: str,
    remote_schema: str,
    remote_table: str,
    remote_db: str,
    remote_password: str,
    remote_user: str = DEFAULT_USER,
    remote_server: str = DEFAULT_SERVER,
    remote_port: int = DEFAULT_PORT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    row_limit: Optional[int] = None,
) -> bool:
    """
    Copy rows from a local table to a remote PostgreSQL database using dblink.

    The function:

    * gets remote column names and types via dblink,
    * determines the intersection of local and remote columns,
    * fetches local rows in the remote column order,
    * builds batched INSERT .. VALUES statements with explicit casts,
    * executes them via dblink_exec inside a transaction per batch.

    Returns
    -------
    bool
        True if all rows were copied successfully, False otherwise.
    """
    _create_server_object(
        conn,
        remote_host,
        remote_db,
        remote_password,
        remote_server,
        remote_user,
        remote_port,
    )

    # 1) Read remote columns and exact types (remote order)
    remote_cols = _get_remote_cols_and_types(
        conn, remote_server, remote_schema, remote_table
    )
    remote_col_names = [name for name, _ in remote_cols]
    remote_type_texts = {name: typ for name, typ in remote_cols}

    # 2) Fetch local rows only for overlapping columns (by name),
    #    in remote order
    #    If schemas differ, still insert only columns that exist
    #    on both sides.
    with conn.cursor() as cur:
        # Determine local columns by querying information_schema
        cur.execute(
            sql.SQL(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """
            ),
            (local_schema, local_table),
        )
        local_cols = {r[0] for r in cur.fetchall()}

    common_cols = [c for c in remote_col_names if c in local_cols]
    if not common_cols:
        raise ValueError(
            "No overlapping columns between local and remote \
                         tables."
        )
    column_identifiers = [sql.Identifier(col) for col in common_cols]

    # 3) Pull local data in the remote column order
    #    (subset = common columns)
    rows = _get_local_rows(
        conn, local_schema, local_table, common_cols, limit=row_limit
    )

    if not rows:
        logging.warning("No rows to copy.")
        return False

    # 4) Chunked multi-VALUES INSERT strings with explicit casts
    #    Example value per cell:  'abc'::text,
    #    '2025-10-07T01:23:45+10'::timestamp with time zone, 'ACTIVE'::my_enum
    def build_insert_values_chunk(chunk: list[tuple[Any, ...]]) -> str:
        value_rows = []
        for r in chunk:
            parts = []
            for col_name, cell in zip(common_cols, r):
                rtype = remote_type_texts[col_name]
                lit = _literal(cell, rtype)
                # Cast every literal to the remote declared type
                # (NULL::type is valid too)
                if lit == "NULL":
                    parts.append(f"NULL::{rtype}")
                else:
                    parts.append(f"{lit}::{rtype}")
            value_rows.append("(" + ", ".join(parts) + ")")

        return ", ".join(value_rows)

    # 5) Execute per-batch on the remote via dblink_exec
    rows_copied = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        values_sql = build_insert_values_chunk(chunk)
        insert_stmt = sql.SQL(
            """INSERT INTO {schema}.{table} ({columns}) \
            VALUES {values};"""
        ).format(
            schema=sql.Identifier(remote_schema),
            table=sql.Identifier(remote_table),
            columns=sql.SQL(", ").join(column_identifiers),
            values=sql.SQL(values_sql),
        )
        try:
            with conn.cursor() as cur:
                insert_stmt_str = insert_stmt.as_string(conn)
                cur.execute("BEGIN;")
                cur.execute(
                    sql.SQL("SELECT dblink_exec(%s, %s);"),
                    (remote_server, insert_stmt_str),
                )
                cur.execute("COMMIT;")
            rows_copied = rows_copied + len(chunk)
            if len(rows) > batch_size:
                logging.info(f"✅ Chunk {i // batch_size + 1} inserted")
        except Exception as e:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK;")
            logging.error(f"❌ Chunk {i // batch_size + 1} failed: {e}")
            # logging.error(insert_stmt)

    conn.commit()

    # _drop_server_object(conn, remote_server)
    logging.info(
        f"Copied {rows_copied}/{len(rows)} row(s) from "
        f"{local_schema}.{local_table} → "
        f"{remote_schema}.{remote_table} via dblink_exec."
    )

    return rows_copied == len(rows)


def _create_server_object(
    conn: psycopg2.extensions.connection,
    remote_host: str,
    remote_db: str,
    remote_password: str,
    remote_server: str,
    remote_user: str = DEFAULT_USER,
    remote_port: int = DEFAULT_PORT,
) -> None:
    """
    Creates (or replaces) a postgres_fdw server and a user mapping used by
    dblink.
    """

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS dblink;")

        _drop_server_object(conn, remote_server)

        cur.execute(
            sql.SQL(
                f"""
            CREATE SERVER IF NOT EXISTS {remote_server}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host %s, port %s, dbname %s);
        """
            ),
            [remote_host, str(remote_port), remote_db],
        )

        cur.execute(
            sql.SQL(
                f"""
            CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
            SERVER {remote_server}
            OPTIONS (user %s, password %s);
        """
            ),
            [remote_user, remote_password],
        )

        cur.execute(
            "SELECT srvname, srvoptions FROM pg_foreign_server",
            [remote_server],
        )
        cur.fetchone()

        conn.commit()


def _drop_server_object(
    conn: psycopg2.extensions.connection,
    server_name: str,
    cascade: bool = CASCADE,
) -> None:
    """
    Drops a postgres_fdw server object.
    """
    with conn.cursor() as cur:
        drop_sql = sql.SQL("DROP SERVER IF EXISTS {srv} {cascade};").format(
            srv=sql.Identifier(server_name),
            cascade=sql.SQL("CASCADE") if cascade else sql.SQL(""),
        )
        cur.execute(drop_sql)
    conn.commit()
