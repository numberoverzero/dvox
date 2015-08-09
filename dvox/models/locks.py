import arrow
from bloop import Column, UUID, DateTime, ConstraintViolation, NotModified
from dvox import engines
from dvox.app import config
from dvox.exceptions import InUse
from dvox.models.types import Position


class ChunkLock(engines.model):
    world = Column(UUID, hash_key=True, name='w')
    chunk = Column(Position, range_key=True, name="c")
    worker = Column(UUID, name='r')
    expires = Column(DateTime, name='e')

    NOT_EXIST = world.is_(None) & chunk.is_(None)

    def acquire(self):
        """Acquire the lock on this chunk.
        Raises `InUse` if a different worker owns the lock on this chunk."""
        now = arrow.now()
        self.expires = now.replace(
            seconds=config["CHUNK_LOCK_TIMEOUT_SECONDS"])

        can_overwrite = (
            # Same worker, just renewing expiration
            (ChunkLock.worker == self.worker) |
            # Existing lock already expired
            (ChunkLock.expires <= now) |
            # No existing lock
            ChunkLock.NOT_EXIST)

        try:
            engines.overwrite.save(self, condition=can_overwrite)
        except ConstraintViolation:
            raise InUse

    def release(self):
        """Release the lock on this chunk.

        Raises `InUse` if the lock was deleted, or a different worker owns
        the lock, or the expire time was updated for this worker."""
        try:
            engines.atomic.delete(self)
        except ConstraintViolation:
            raise InUse

    @property
    def current(self):
        """Return the current lock or None."""
        existing_lock = ChunkLock(world=self.world, chunk=self.chunk)
        try:
            engines.consistent.load(existing_lock)
        except NotModified:
            return None
        else:
            return existing_lock
