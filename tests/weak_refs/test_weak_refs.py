from dataclasses import dataclass
import gc
import pytest

from dcorm.weak_refs import WeakKeyDict


@pytest.fixture
def object_one():
    f = Foo(a=1)
    return f


@pytest.fixture
def data_one():
    return {"a": 7, "b": 9}


@pytest.fixture
def weak_key_dict():
    return WeakKeyDict()


@dataclass
class Foo:
    a: int
    b: int = 42


def test_weak_key_dict_with_object_has_len_one(weak_key_dict, object_one, data_one):
    weak_key_dict[object_one] = data_one
    assert len(weak_key_dict) == 1


def test_stored_key_is_in_weak_key_dict(weak_key_dict, object_one, data_one):
    weak_key_dict[object_one] = data_one
    assert object_one in weak_key_dict


def test_weak_key_dict_stores_value(weak_key_dict, object_one, data_one):
    weak_key_dict[object_one] = data_one
    assert weak_key_dict[object_one] == data_one


def test_gc_removes_object_reference(weak_key_dict):
    # Can't use a fixture for the object here because it wouldn't
    # get garbage collected.
    f = Foo(a=1)
    weak_key_dict[f] = 42

    assert len(weak_key_dict) == 1

    del f
    gc.collect()

    assert len(weak_key_dict) == 0


def test_weak_key_dict_raises_key_error_on_unknown_object(weak_key_dict, object_one):
    with pytest.raises(KeyError):
        weak_key_dict[object_one]
