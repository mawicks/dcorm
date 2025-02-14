from __future__ import annotations
from dcorm.types import Connection, ConnectionFactory


class ConnectionContextMgr:
    _connection: Connection
    _pool: ConnectionPool

    def __init__(self, connection: Connection, pool: ConnectionPool):
        self._connection = connection
        self._pool = pool

    def __enter__(self) -> Connection:
        return self._connection

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._connection.commit()
        else:
            self._connection.rollback()

        self._pool.release(self._connection)


class ConnectionPool:
    _connection_factory: ConnectionFactory
    _pool: list[Connection]

    def __init__(self, connection_factory, size=16):
        self._size = size
        self._connection_factory = connection_factory
        self._pool = [connection_factory() for _ in range(size)]

    def use(self) -> ConnectionContextMgr:
        try:
            connection = self._pool.pop()
        except IndexError:
            connection = self._connection_factory()
        return ConnectionContextMgr(connection, self)

    def release(self, connection: Connection):
        if len(self._pool) < self._size:
            self._pool.append(connection)
