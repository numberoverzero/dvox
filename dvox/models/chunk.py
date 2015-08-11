import enum
from bloop import Column, UUID, Integer
from dvox import engines
from dvox.models.types import Position, Enum
from dvox.models.locks import ChunkLock
from dvox.exceptions import InUse

_DEFAULT_SEED = 0


@enum.unique
class ChunkType(enum.Enum):
    EMPTY = 0
    DATA = 1
    REF = 2


class Chunk(engines.model):
    world = Column(UUID, hash_key=True, name='w')
    coords = Column(Position, range_key=True, name="c")
    seed = Column(Integer, name='s')
    type = Column(Enum(ChunkType), name='t')

    def __init__(self, **kwargs):
        if 'seed' not in kwargs:
            kwargs['seed'] = _DEFAULT_SEED
        if 'type' not in kwargs:
            kwargs['type'] = ChunkType.EMPTY
        super().__init__(**kwargs)

    def lock(self, worker):
        lock = ChunkLock(world=self.world, chunk=self.coords, worker=worker)
        try:
            lock.acquire()
        except InUse:
            return None
        else:
            return lock
