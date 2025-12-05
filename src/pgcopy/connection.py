import base64
import hashlib
import io
import select
import socket
import threading
from typing import NoReturn, Tuple, cast

import paramiko
import psycopg2

from pgcopy.config import ssh_fingerprint

"""SSH tunnel and PostgreSQL connection helpers.

This module establishes an SSH tunnel to a remote host and opens a
psycopg2 PostgreSQL connection through that tunnel.
"""

DEFAULT_HOST = "localhost"
DEFAULT_DATABASE_NAME = "postgres"
DEFAULT_DATABASE_USER = "postgres"
DEFAULT_PORT = 5432


def _forward_tunnel(
    remote_host: str,
    remote_port: int,
    transport: paramiko.transport.Transport,
    local_host: str = DEFAULT_HOST,
    local_port: int = DEFAULT_PORT,
) -> NoReturn:
    """
    Establishes a local TCP listener and forwards all traffic through an SSH
    transport as a direct-tcpip channel to the specified remote host and port.

    Parameters:
        remote_host (str): Target host reached via the SSH transport.
        remote_port (int): Target port on the remote host.
        transport (paramiko.Transport): Active SSH transport used to open \
        channels.
        local_host (str): Local bind address for the forwarder. Defaults to \
        "localhost".
        local_port (int): Local port to listen on. Defaults to 5432.

    Behaviour:
        - Creates a listening socket on (local_host, local_port).
        - For each incoming client connection, opens an SSH channel of type
          "direct-tcpip" to (remote_host, remote_port).
        - Relays data bidirectionally between the local client socket and the
          SSH channel until either side closes the connection.
        - Handles each client connection in a separate daemon thread.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((local_host, local_port))
    sock.listen(100)

    def handler(client_sock: socket.socket) -> None:
        chan = transport.open_channel(
            "direct-tcpip",
            (remote_host, remote_port),
            client_sock.getsockname(),
        )
        if chan is None:
            print("Could not open SSH tunnel")
            return

        while True:
            r, _, _ = select.select([client_sock, chan], [], [])
            if client_sock in r:
                data = client_sock.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                client_sock.send(data)

        chan.close()
        client_sock.close()

    while True:
        client_sock, _ = sock.accept()
        threading.Thread(
            target=handler, args=(client_sock,), daemon=True
        ).start()


def create_pg_connection(
    ssh_host: str,
    ssh_user: str,
    ssh_key: str,
    remote_host: str,
    db_password: str,
    expected_fingerprint: str = ssh_fingerprint,
    remote_port: int = DEFAULT_PORT,
    db_name: str = DEFAULT_DATABASE_NAME,
    db_user: str = DEFAULT_DATABASE_USER,
    local_host: str = DEFAULT_HOST,
    local_port: int = DEFAULT_PORT,
) -> Tuple[psycopg2.extensions.connection, paramiko.SSHClient]:
    """
    Create a PostgreSQL connection via an SSH tunnel.

    The function:

    * connects to an SSH bastion using an in-memory private key,
    * starts a TCP listener that forwards to the remote PostgreSQL instance,
    * returns both the psycopg2 connection and the underlying SSH client.

    Parameters
    ----------
    ssh_host : str
        SSH bastion host name or IP.
    ssh_user : str
        SSH user used to log into the bastion.
    ssh_key : str
        Private key material in OpenSSH format (string, not file path).
    remote_host : str
        Host name or IP of the remote PostgreSQL server.
    db_password : str
        Password for the PostgreSQL user.
    expected_fingerprint : str
        The expected fingerprint of the SSH server.
    remote_port : int, optional
        Remote PostgreSQL port, default is 5432.
    db_name : str, optional
        PostgreSQL database name, default is 'postgres'.
    db_user : str, optional
        PostgreSQL user name, default is 'postgres'.
    local_host : str, optional
        Local bind address for the tunnel, default is 'localhost'.
    local_port : int, optional
        Local bind port for the tunnel, default is 5432.

    Returns
    -------
    tuple
        (psycopg2 connection, paramiko.SSHClient)
    """

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key_stream = io.StringIO(ssh_key)
    private_key = paramiko.RSAKey.from_private_key(key_stream)
    client.connect(ssh_host, username=ssh_user, pkey=private_key)

    server_key = cast(
        paramiko.transport.Transport, client.get_transport()
    ).get_remote_server_key()

    # fingerprint = hashlib.sha256(server_key.asbytes()).hexdigest()
    digest = hashlib.sha256(server_key.asbytes()).digest()
    fingerprint = base64.b64encode(digest).rstrip(b"=").decode("ascii")

    if fingerprint != expected_fingerprint:
        client.close()
        raise ValueError(f"Unexpected SSH host key fingerprint: {fingerprint}")

    transport = client.get_transport()

    threading.Thread(
        target=_forward_tunnel,
        args=(remote_host, remote_port, transport, local_host, local_port),
        daemon=True,
    ).start()

    print(f"SSH Tunnel running on {local_host}:{local_port}")

    conn = psycopg2.connect(
        host=local_host,
        port=local_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        options="-c client_encoding=UTF8",
    )

    return conn, client
