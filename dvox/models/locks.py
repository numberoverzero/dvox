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

    @classmethod
    def acquire(cls, world, chunk, worker):
        now = arrow()
        lock = ChunkLock(world=world, chunk=chunk,
                         worker=worker, acquire=now)
        not_exist = ChunkLock.world.is_(None) & ChunkLock.chunk.is_(None)
        try:
            engine.save(lock, condition=not_exist)
        except ConstraintViolation:
            # This chunk is already locked.  If the given worker matches
            # the existing lock's worker, we can update the acquire time to
            # renew the existing lock.

            # Otherwise, the lock is held by a different worker; if the
            # acquire time is older than the configurable update interval,
            # that worker may have died.  We can forcefully acquire the
            # lock at that point.
            existing_lock = ChunkLock(world=world, chunk=chunk)
            try:
                engine.load(existing_lock)
            except NotModified:
                # The lock that existed when we tried to save above is
                # no longer there.  A periodic cleanup may have swept it
                # up before we could compare acquire time.

                # At this point we should simply retry the acquire.
                raise RetryOperation()
            elapsed = (existing_lock.acquire - now).total_seconds()

            # The worker that's trying to acquire the lock is
            # the same one that previously had the lock.  Update
            # acquire time and attempt to save
            if existing_lock.worker == lock.worker:
                existing_lock.acquire = now
                try:
                    atomic_engine.save(existing_lock)
                except ConstraintViolation:
                    # The worker or acquire time on the existing lock
                    # has changed.  Signal that the acquire can be
                    # retried.
                    raise RetryOperation()
                else:
                    return existing_lock
            # The worker is different, but hasn't checked in within the
            # CHUNK_LOCK_TIMEOUT_SECONDS, and can be re-acquired by a new
            # worker.
            elif elapsed >= config["CHUNK_LOCK_TIMEOUT_SECONDS"]:
                existing_lock.worker = lock.worker
                try:
                    atomic_engine.save(existing_lock)
                except ConstraintViolation:
                    # The worker or acquire time on the existing lock
                    # has changed.  Signal that the acquire can be
                    # retried.
                    raise RetryOperation()
                else:
                    return existing_lock
            # Existing lock is owned by a different worker, and was last
            # acquired within the lock timeout.  Can't be acquired right now.
            else:
                raise InUse()
        # No previous lock existed, save to return.
        else:
            return lock
