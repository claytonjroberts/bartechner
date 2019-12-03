from collections import defaultdict
from dataclasses import dataclass, field, fields
from threading import Lock, RLock


@dataclass()
class SLockInner:
    "An actual lock, works with the SLock parent"
    lock: RLock = field(default_factory=RLock)
    _lock_wait: Lock = field(default_factory=Lock)
    _lock_check: Lock = field(default_factory=Lock)
    waits: int = 0

    def __enter__(self, *args, **kwargs):
        self.acquire(*args, *kwargs)
        # self.lock.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        self.release()
        # self.lock.__exit__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({'un' if self.isLocked else ''}locked waits={self.waits})>"

    @property
    def isDead(self) -> bool:
        assert self.waits >= 0
        return self.waits == 0

    @property
    def isLocked(self) -> bool:
        try:
            return self.lock.locked()
        except AttributeError:
            # Assume RLock
            if self.lock.acquire(False):
                # Lock can be aquired, so not locked
                self.lock.release()
                return False
            else:
                return True

    def acquire(self, *args, **kwargs):
        with self._lock_wait:
            self.waits += 1
        return self.lock.acquire(*args, **kwargs)

    def release(self, *args, **kwargs):
        with self._lock_wait:
            self.waits -= 1
        self.lock.release(*args, **kwargs)


@dataclass
class SLock:
    """Lock that uses specialized keys to prevent the same 'key', but allows different or new
    """

    _lock_create: Lock = field(default_factory=RLock)
    _lock_increment: Lock = field(default_factory=RLock)

    locks: defaultdict = field(default_factory=lambda: defaultdict(SLockInner))

    def __getitem__(self, key):
        key = self.__keytransform__(key)
        with self._lock_create:
            # items = list(self.locks.items())
            # List here to prevent change in size during iteration ->
            for k, lockInner in list(self.locks.items()):
                # Clear out dead inner locks -->
                if lockInner.isDead:
                    del self.locks[k]

            return self.locks[key]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.locks})>"

    # def __setitem__(self, key, value):
    #     raise NotImplementedError()
    #     self.locks[self.__keytransform__(key)] = value
    #
    # def __delitem__(self, key):
    #     del self.locks[self.__keytransform__(key)]
    #
    # def __iter__(self):
    #     return iter(self.locks)
    #
    # def __len__(self):
    #     return len(self.locks)

    def __keytransform__(self, key):
        return key
