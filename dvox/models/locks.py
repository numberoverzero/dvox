import arrow
from bloop import Column, UUID, DateTime, ConstraintViolation, NotModified
from dvox.app import engine, atomic_engine, config
from dvox.exceptions import InUse, RetryOperation
from dvox.models.types import Position


class ChunkLock(engine.model):
    world = Column(UUID, hash_key=True, name='w')
    chunk = Column(Position, range_key=True, name="c")
    worker = Column(UUID, name='r')
    acquire = Column(DateTime, name='a')

    def acquire(self):
        previous_acquire = self.acquire
        self.acquire = arrow.now()
        not_exist = ChunkLock.world.is_(None) & ChunkLock.chunk.is_(None)
        try:
            engine.save(self, condition=not_exist)
        except ConstraintViolation:
            existing_lock = self._current()

            # The lock that existed when we tried to save above is
            # no longer there.  A periodic cleanup may have swept it
            # up before we could compare acquire time, or it was released.
            # At this point we should simply retry the acquire.
            if existing_lock is None:
                self.acquire = previous_acquire
                raise RetryOperation()

            elapsed = (existing_lock.acquire - self.acquire).total_seconds()

            # The worker that's trying to acquire the lock is
            # the same one that previously had the lock.  Update
            # acquire time and attempt to save
            if existing_lock.worker == self.worker:
                existing_lock.acquire = self.acquire
                try:
                    atomic_engine.save(existing_lock)
                except ConstraintViolation:
                    # The worker or acquire time on the existing lock
                    # has changed.  Signal that the acquire can be
                    # retried.
                    self.acquire = previous_acquire
                    raise RetryOperation()
            # The worker is different, but hasn't checked in within the
            # CHUNK_LOCK_TIMEOUT_SECONDS, and can be re-acquired by a new
            # worker.  Try to take it over and update acquire time
            elif elapsed >= config["CHUNK_LOCK_TIMEOUT_SECONDS"]:
                existing_lock.worker = self.worker
                existing_lock.acquire = self.acquire
                try:
                    atomic_engine.save(existing_lock)
                except ConstraintViolation:
                    # The worker or acquire time on the existing lock
                    # has changed.  Signal that the acquire can be
                    # retried.
                    self.acquire = previous_acquire
                    raise RetryOperation()
            # Existing lock is owned by a different worker, and was last
            # acquired within the lock timeout.  Can't be acquired right now.
            else:
                self.acquire = previous_acquire
                raise InUse()
        # Success

    def _current(self):
        """
        Returns the current lock for the lock's world/position,
        or None if one does not exist.
        """
        current_lock = ChunkLock(world=self.world, chunk=self.chunk)
        try:
            engine.load(current_lock)
        except NotModified:
            return None
        else:
            return current_lock
