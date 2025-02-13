from collections import namedtuple

from typing import Any, Dict
import weakref

# Until dataclasses with slots are more widely available,
# a namedtuple is used here to keep the memory footprint small.
ObjectData = namedtuple("ObjectData", ["weakref", "data"])


class WeakKeyDict:
    # The keys are the object ids returned by id().
    # The values are instances of ObjectData.
    data: Dict[int, ObjectData]

    def __init__(self):
        self.data = {}

    def __setitem__(self, instance: object, data: Any):
        oid = id(instance)

        def callback(ref):
            if self.data[oid].weakref is ref:
                del self.data[oid]
            else:
                raise RuntimeError(
                    f"Ref in callback {ref} doesn't match stored ref {self.data[oid].weakref}"
                )

        ref = weakref.ref(instance, callback)
        self.data[oid] = ObjectData(weakref=ref, data=data)

    def __contains__(self, instance: object):
        oid = id(instance)

        return oid in self.data and (self.data[oid].weakref() is not None)

    def __getitem__(self, instance: object) -> Any:
        object_data = self.data[id(instance)]
        if object_data.weakref() is not None:
            return object_data.data
        else:
            return None

    def __len__(self):
        return len(self.data)
