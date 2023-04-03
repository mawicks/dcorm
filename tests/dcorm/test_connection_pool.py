from typing import cast, Iterable, Tuple

import pytest

from sandbox.dcorm.connection_pool import ConnectionPool


class MockCursor:
    def execute(self, sql: str, parameters: Tuple[object, ...] = ()):
        pass

    def executemany(self, sql: str, parameters: Iterable[Tuple[object, ...]]):
        pass

    def executescript(self, sql: str):
        pass

    def fetchall(self) -> list[tuple[object, ...]]:
        return [()]


class MockConnection:
    idx: int
    cursor_called = False
    commit_called = False
    rollback_called = False

    def __init__(self, idx: int):
        self.idx = idx

    def cursor(self) -> MockCursor:
        self.cursor_called = True
        return MockCursor()

    def commit(self):
        self.commit_called = True
        return

    def rollback(self):
        self.rollback_called = True
        return


@pytest.fixture
def connections():
    return [MockConnection(i) for i in range(6)]


@pytest.fixture
def connection_factory(connections):
    def factory():
        return connections.pop(0)

    return factory


def test_connection_pool_creates_size_connections(connections, connection_factory):
    # The connection_factory has access to six objects, but the
    # pool is instructed to use only 2.
    ConnectionPool(connection_factory, size=2)

    # The remaining four should be untouched.
    assert len(connections) == 4


def test_connection_uses_all_and_only_pooled_objects(connection_factory):
    cp = ConnectionPool(connection_factory, size=2)
    used_connection_ids = []
    # With just two objects in the pool, use each object several times
    # Record the ids of the objects that get assigned and confirm
    # they are in the expected range.
    for _ in range(6):
        with cp.use() as connection0, cp.use() as connection1:
            used_connection_ids.append(cast(MockConnection, connection0).idx)
            used_connection_ids.append(cast(MockConnection, connection1).idx)
            connection0.cursor()
            connection1.cursor()
    # We expect both objects to have been used.
    assert max(used_connection_ids) == 1
    assert min(used_connection_ids) == 0


def test_commit_called_on_all_and_only_pooled_objects(connections, connection_factory):
    # Record the objects before they get placed in the pool:
    connections = connections.copy()
    cp = ConnectionPool(connection_factory, size=2)

    # Again, use each object several times
    for _ in range(6):
        with cp.use() as connection0, cp.use() as connection1:
            connection0.cursor()
            connection1.cursor()

    # We expect commit() to have been called on the first two objects.
    assert all(c.commit_called for c in connections[:2])
    # We do not expect commit() to have been called on the remaining objects (not
    # in the pool)
    assert not any(c.commit_called for c in connections[2:])

    # We do not expect rollback() to have been called on anything.
    assert not any(c.rollback_called for c in connections)


def test_rollback_called_on_all_pooled_objects(connections, connection_factory):
    # Record the objects before they get placed in the pool:
    connections = connections.copy()
    cp = ConnectionPool(connection_factory, size=2)

    # Again, use each object several times
    try:
        for _ in range(6):
            with cp.use() as connection0, cp.use() as connection1:
                connection0.cursor()
                connection1.cursor()
                raise ValueError("Some exception")
    except ValueError:
        pass

    # We expect rollback() called on first two objects
    assert all([c.rollback_called for c in connections[:2]])
