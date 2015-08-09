import arrow
from bloop import Column, UUID, DateTime, ConstraintViolation, NotModified

from dvox.app import config
from dvox.engines import atomic, consistent, engine, overwrite
from dvox.exceptions import InUse, RetryOperation, Expired
from dvox.models.types import Position


class ChunkLock(engine.model):
    world = Column(UUID, hash_key=True, name='w')
    chunk = Column(Position, range_key=True, name="c")
    worker = Column(UUID, name='r')
    time = Column(DateTime, name='t')

    def acquire(self):
        self.time = arrow.now()
        try:
            overwrite.save(self, condition=ChunkLock.NOT_EXIST)
        except ConstraintViolation:
            existing_lock = self._current()

            # The lock that existed when we tried to save above is
            # no longer there.  A periodic cleanup may have swept it
            # up before we could compare acquire time, or it was released.
            # At this point we should simply retry the acquire.
            if existing_lock is None:
                raise RetryOperation()

            elapsed = (self.time - existing_lock.time).total_seconds()

            # The worker hasn't checked in within the timeout; release the
            # old lock, and signal a retry.
            if elapsed >= config["CHUNK_LOCK_TIMEOUT_SECONDS"]:
                existing_lock.release()
                raise RetryOperation()
            # The worker that's trying to acquire the lock is
            # the same one that previously had the lock.  Renew the lock.
            elif existing_lock.worker == self.worker:
                # Will recurse into acquire again if the lock was
                # deleted after the load above.
                existing_lock.renew()
            # Existing lock is owned by a different worker, and was last
            # acquired within the lock timeout.  Can't be acquired right now.
            else:
                raise InUse()
        # Success

    def release(self):
        try:
            # Only delete the EXACT lock we have - any change and we
            # would be deleting the wrong lock
            atomic.delete(self)
        except ConstraintViolation:
            # The lock has changed - either it expired and was overwritten,
            # or the same worker holds it and it was refreshed.

            # Either way, we shouldn't delete it since self refers to a lock
            # that no longer exists.
            pass

    def renew(self):
        self.time = arrow.now()
        try:
            atomic.save(self)
        except ConstraintViolation:
            # The lock was deleted, another worker took it over, or
            # it was renewed before this call.
            existing_lock = self._current()

            # The lock was deleted - try to acquire it.
            if existing_lock is None:
                try:
                    self.acquire()
                except InUse:
                    # The lock was acquired after the last load of None.
                    # We don't raise InUse because the lock that won may be the
                    # same worker - in which case, this is a retryable renew.
                    raise RetryOperation()
            # Another worker took it over.
            elif existing_lock.worker != self.worker:
                raise Expired()
            # Lock exists, same worker, acquire time is different.  Signal
            # that the renew can be retried.
            else:
                raise RetryOperation()

    def _current(self):
        """
        Returns the current lock for the lock's world/position,
        or None if one does not exist.
        """
        existing_lock = ChunkLock(world=self.world, chunk=self.chunk)
        try:
            consistent.load(existing_lock)
        except NotModified:
            return None
        else:
            return existing_lock

ChunkLock.NOT_EXIST = ChunkLock.world.is_(None) & ChunkLock.chunk.is_(None)
