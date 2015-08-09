import arrow
from bloop import Column, UUID, DateTime, ConstraintViolation, NotModified
from dvox import engines
from dvox.app import config
from dvox.exceptions import InUse, Expired
from dvox.models.types import Position


class ChunkLock(engines.model):
    world = Column(UUID, hash_key=True, name='w')
    chunk = Column(Position, range_key=True, name="c")
    worker = Column(UUID, name='r')
    expires = Column(DateTime, name='e')

    def acquire(self):
        """Acquire the lock on this chunk.
        Raises `InUse` if a different worker owns the lock on this chunk."""
        now = arrow.now()
        self.expires = now.replace(
            seconds=config["CHUNK_LOCK_TIMEOUT_SECONDS"])
        try:
            engines.overwrite.save(self, condition=ChunkLock.NOT_EXIST)
        except ConstraintViolation:
            existing_lock = self.current()

            # Although there was a conflict with an existing lock,
            # the next load found no lock.  Try again.
            if existing_lock is None:
                self.acquire()

            # The last worker with the lock didn't check in before
            # the timeout.  Try again.
            elif now >= existing_lock.expires:
                try:
                    existing_lock.release()
                # After the current lock was loaded, it was modified
                # before we could release it.  Try again.
                except InUse:
                    pass
                self.acquire()

            # The existing lock was held by the same worker.  Try to renew.
            elif existing_lock.worker == self.worker:
                try:
                    self.renew()
                # After the current lock was loaded, it was modified (or
                # deleted) before we could renew.  Try again.
                except Expired:
                    self.acquire()

            # Existing lock is owned by a different worker and hasn't expired.
            else:
                raise InUse
        # Success

    def release(self):
        """Release the lock on this chunk.

        Raises `InUse` if the lock was deleted, or a different worker owns
        the lock, or the expire time was updated for this worker."""
        try:
            engines.atomic.delete(self)
        except ConstraintViolation:
            raise InUse

    def renew(self):
        """Update the lock's expire time.

        Raises `Expired` if the lock was deleted, or a different worker
        owns the lock."""
        self.expires = arrow.now().replace(
            seconds=config["CHUNK_LOCK_TIMEOUT_SECONDS"])
        try:
            same_worker = ChunkLock.worker == self.worker
            engines.overwrite.save(self, condition=same_worker)
        except ConstraintViolation:
            # The lock was deleted, or another worker took it over.
            raise Expired

    def current(self):
        """Return the current lock or None."""
        existing_lock = ChunkLock(world=self.world, chunk=self.chunk)
        try:
            engines.consistent.load(existing_lock)
        except NotModified:
            return None
        else:
            return existing_lock

ChunkLock.NOT_EXIST = ChunkLock.world.is_(None) & ChunkLock.chunk.is_(None)
