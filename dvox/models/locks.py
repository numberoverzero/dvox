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
        now = arrow.now()
        self.expires = now.replace(
            seconds=config["CHUNK_LOCK_TIMEOUT_SECONDS"])
        try:
            engines.overwrite.save(self, condition=ChunkLock.NOT_EXIST)
        except ConstraintViolation:
            existing_lock = self.current()

            # The lock that existed when we tried to save above is
            # no longer there.  A periodic cleanup may have swept it
            # up, or it was manually released.
            # At this point we should simply retry the acquire.
            if existing_lock is None:
                self.acquire()

            # The worker hasn't checked in within the timeout; release the
            # old lock and retry.
            elif now >= existing_lock.expires:
                try:
                    existing_lock.release()
                # Suppress InUse, since it means the actual lock was
                # modified after the existing_lock was loaded.  A retry here
                # may succeed, if another thread released before us.  At worst,
                # the acquire call will fall down to the last else below, and
                # then throw InUse.
                except InUse:
                    pass
                self.acquire()

            # The worker that's trying to acquire the lock is
            # the same one that previously had the lock.  Renew the lock.
            elif existing_lock.worker == self.worker:
                try:
                    self.renew()
                # Some expired locks can be retried.  If the lock was deleted
                # after the load above, it can be acquired, but renew will
                # refuse because the lock isn't exactly the same.
                # This isn't a big deal; if the lock was taken over by
                # another worker, the next acquire will fail with InUse below.
                except Expired:
                    self.acquire()

            # Existing lock is owned by a different worker, and was last
            # acquired within the lock timeout.  Can't be acquired right now.
            else:
                raise InUse
        # Success

    def release(self):
        try:
            # Only delete the EXACT lock we have - any change and we
            # would be deleting the wrong lock
            engines.atomic.delete(self)
        except ConstraintViolation:
            raise InUse

    def renew(self):
        self.expires = arrow.now().replace(
            seconds=config["CHUNK_LOCK_TIMEOUT_SECONDS"])
        try:
            same_worker = ChunkLock.worker == self.worker
            engines.overwrite.save(self, condition=same_worker)
        except ConstraintViolation:
            # The lock was deleted, or another worker took it over.
            raise Expired

    def current(self):
        """
        Returns the current lock for the lock's world/position,
        or None if one does not exist.
        """
        existing_lock = ChunkLock(world=self.world, chunk=self.chunk)
        try:
            engines.consistent.load(existing_lock)
        except NotModified:
            return None
        else:
            return existing_lock

ChunkLock.NOT_EXIST = ChunkLock.world.is_(None) & ChunkLock.chunk.is_(None)
