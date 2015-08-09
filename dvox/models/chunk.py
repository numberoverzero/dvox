from bloop import Column, UUID, Integer
from dvox import engines
from dvox.models.types import Position
from dvox.models.locks import ChunkLock
from dvox.exceptions import InUse

_DEFAULT_SEED = 0


class Chunk(engines.model):
    world = Column(UUID, hash_key=True, name='w')
    coords = Column(Position, range_key=True, name="c")
    seed = Column(Integer, name='s')

    def __init__(self, **kwargs):
        if 'seed' not in kwargs:
            kwargs['seed'] = _DEFAULT_SEED
        super().__init__(**kwargs)

    def lock(self, worker):
        """
        Returns a lock if one can be acquired.
        Otherwise, returns None.
        """
        lock = ChunkLock(world=self.world, chunk=self.coords, worker=worker)
        try:
            lock.acquire()
        except InUse:
            return None
        else:
            return lock
