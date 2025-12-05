import base64
import hashlib
from unittest.mock import MagicMock, patch

import pytest

from pgcopy.connection import _forward_tunnel, create_pg_connection


@patch("pgcopy.connection.select.select")
@patch("pgcopy.connection.socket.socket")
def test_forward_tunnel_handles_one_connection_and_exits(
    mock_socket, mock_select
):
    transport = MagicMock()
    chan = MagicMock()
    transport.open_channel.return_value = chan

    client_sock = MagicMock()
    client_sock.recv.return_value = b""
    client_sock.getpeername.return_value = ("127.0.0.1", 1111)

    sock = MagicMock()
    mock_socket.return_value = sock

    state = {"first": True}

    def accept_side_effect(*args, **kwargs):
        if state["first"]:
            state["first"] = False
            return (client_sock, ("127.0.0.1", 12345))
        raise KeyboardInterrupt

    sock.accept.side_effect = accept_side_effect

    def select_side_effect(readers, *_args, **_kwargs):
        return ([client_sock], [], [])

    mock_select.side_effect = select_side_effect

    with pytest.raises(KeyboardInterrupt):
        _forward_tunnel("remote-host", 5432, transport, local_port=0)

    sock.bind.assert_called_once()
    sock.listen.assert_called_once()
    transport.open_channel.assert_called_once()
    client_sock.close.assert_called_once()
    chan.close.assert_called_once()


@patch("pgcopy.connection.psycopg2.connect")
@patch("pgcopy.connection.paramiko.RSAKey.from_private_key")
@patch("pgcopy.connection.paramiko.SSHClient")
def test_create_pg_connection_opens_ssh_and_returns_connection(
    mock_ssh_client_cls,
    mock_rsa_from_key,
    mock_pg_connect,
):
    raw = b"dummy-server-key"
    ssh_client = MagicMock()
    mock_ssh_client_cls.return_value = ssh_client
    transport = MagicMock()
    ssh_client.get_transport.return_value = transport
    server_key = MagicMock()
    server_key.asbytes.return_value = raw
    transport.get_remote_server_key.return_value = server_key
    mock_pg_connect.return_value = "CONN"

    # expected_fingerprint = hashlib.sha256(b"dummy-server-key").hexdigest()
    digest = hashlib.sha256(raw).digest()
    expected_fingerprint = (
        base64.b64encode(digest).rstrip(b"=").decode("ascii")
    )

    conn, client = create_pg_connection(
        ssh_host="ssh-host",
        ssh_user="user",
        ssh_key="PRIVATE_KEY",
        remote_host="db-host",
        db_password="pwd",
        expected_fingerprint=expected_fingerprint,
        db_name="postgres",
    )

    mock_ssh_client_cls.assert_called_once()
    ssh_client.connect.assert_called_once()
    mock_pg_connect.assert_called_once()
    assert conn == "CONN"
    assert client is ssh_client


@patch("pgcopy.connection.psycopg2.connect")
@patch("pgcopy.connection.paramiko.RSAKey.from_private_key")
@patch("pgcopy.connection.paramiko.SSHClient")
def test_fingerprint_mismatch_raises_value_error(
    mock_ssh_client_cls,
    mock_rsa_from_key,
    mock_pg_connect,
):
    ssh_client = MagicMock()
    mock_ssh_client_cls.return_value = ssh_client

    # Transport & Key Mock
    transport = MagicMock()
    ssh_client.get_transport.return_value = transport

    server_key = MagicMock()
    server_key.asbytes.return_value = b"dummy"
    transport.get_remote_server_key.return_value = server_key

    from pgcopy.connection import create_pg_connection

    with pytest.raises(ValueError):
        create_pg_connection(
            ssh_host="ssh-host",
            ssh_user="user",
            ssh_key="PRIVATE_KEY",
            remote_host="db-host",
            db_password="pwd",
            expected_fingerprint="wrong",
            db_name="postgres",
        )


@patch("pgcopy.connection.threading.Thread")
@patch("pgcopy.connection.socket.socket")
def test_forward_tunnel_handles_missing_channel(
    mock_socket_cls,
    mock_thread_cls,
):
    transport = MagicMock()

    class DummyClientSock:
        def getsockname(self):
            return ("127.0.0.1", 5000)

        def close(self):
            pass

    sock = MagicMock()
    mock_socket_cls.return_value = sock
    sock.setsockopt.return_value = None
    sock.bind.return_value = None
    sock.listen.return_value = None
    sock.accept.side_effect = [
        (DummyClientSock(), ("peer", 1234)),
        KeyboardInterrupt,
    ]

    transport.open_channel.return_value = None

    class DummyThread:
        def __init__(self, target, args=(), daemon=None):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            self.target(*self.args)

    mock_thread_cls.side_effect = lambda target, args, daemon: DummyThread(
        target, args, daemon
    )

    with pytest.raises(KeyboardInterrupt):
        _forward_tunnel(
            remote_host="remote",
            remote_port=5432,
            transport=transport,
            local_host="127.0.0.1",
            local_port=5000,
        )


@patch("pgcopy.connection.select.select")
@patch("pgcopy.connection.threading.Thread")
@patch("pgcopy.connection.socket.socket")
def test_forward_tunnel_copies_data_between_client_and_channel(
    mock_socket_cls,
    mock_thread_cls,
    mock_select,
):
    transport = MagicMock()

    class DummyClientSock:
        def __init__(self):
            self.recv_calls = 0
            self.sent = []

        def getsockname(self):
            return ("127.0.0.1", 5000)

        def recv(self, n):
            self.recv_calls += 1
            if self.recv_calls == 1:
                return b"hello"
            return b""

        def send(self, data):
            self.sent.append(data)

        def close(self):
            pass

    class DummyChannel:
        def __init__(self):
            self.recv_calls = 0
            self.sent = []

        def send(self, data):
            self.sent.append(data)

        def recv(self, n):
            self.recv_calls += 1
            if self.recv_calls == 1:
                return b"world"
            return b""

        def close(self):
            pass

    client_sock = DummyClientSock()
    chan = DummyChannel()
    transport.open_channel.return_value = chan

    sock = MagicMock()
    mock_socket_cls.return_value = sock
    sock.setsockopt.return_value = None
    sock.bind.return_value = None
    sock.listen.return_value = None
    sock.accept.side_effect = [
        (client_sock, ("peer", 1234)),
        KeyboardInterrupt,
    ]

    events = [
        [client_sock],
        [chan],
        [client_sock],
    ]

    def fake_select(read_list, write_list, exc_list):
        if not events:
            raise KeyboardInterrupt()
        r = events.pop(0)
        return (r, [], [])

    mock_select.side_effect = fake_select

    class DummyThread:
        def __init__(self, target, args=(), daemon=None):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            self.target(*self.args)

    mock_thread_cls.side_effect = lambda target, args, daemon: DummyThread(
        target, args, daemon
    )

    with pytest.raises(KeyboardInterrupt):
        _forward_tunnel(
            remote_host="remote",
            remote_port=5432,
            transport=transport,
            local_host="127.0.0.1",
            local_port=5000,
        )

    assert b"hello" in chan.sent
    assert b"world" in client_sock.sent


@patch("pgcopy.connection.threading.Thread")
@patch("pgcopy.connection.select.select")
@patch("pgcopy.connection.socket.socket")
def test_forward_tunnel_breaks_when_remote_channel_closes(
    mock_socket_cls,
    mock_select,
    mock_thread_cls,
):
    transport = MagicMock()
    chan = MagicMock()
    client_sock = MagicMock()

    client_sock.getsockname.return_value = ("127.0.0.1", 5000)
    transport.open_channel.return_value = chan

    sock = MagicMock()
    mock_socket_cls.return_value = sock
    sock.setsockopt.return_value = None
    sock.bind.return_value = None
    sock.listen.return_value = None

    sock.accept.side_effect = [
        (client_sock, ("peer", 1234)),
        KeyboardInterrupt,
    ]

    def fake_select(read_list, *_):
        return ([chan], [], [])

    mock_select.side_effect = fake_select

    chan.recv.return_value = b""

    class DummyThread:
        def __init__(self, target, args=(), daemon=None):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            self.target(*self.args)

    mock_thread_cls.side_effect = lambda target, args, daemon: DummyThread(
        target, args, daemon
    )

    with pytest.raises(KeyboardInterrupt):
        _forward_tunnel(
            remote_host="remote",
            remote_port=5432,
            transport=transport,
            local_host="127.0.0.1",
            local_port=5000,
        )

    chan.close.assert_called_once()
    client_sock.close.assert_called_once()
