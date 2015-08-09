import uuid
from bloop import Column, UUID, Integer, ConstraintViolation
from dvox.app import config
from dvox.engines import engine
from dvox.exceptions import CreateFailed

_DEFAULT_SEED = 0


class World(engine.model):
    id = Column(UUID, hash_key=True)
    seed = Column(Integer, name="s")

    def __init__(self, **kwargs):
        if 'seed' not in kwargs:
            kwargs['seed'] = _DEFAULT_SEED
        super().__init__(**kwargs)

    @classmethod
    def unique(cls):
        retries = config["CREATE_RETRIES"]
        does_not_exist = cls.id.is_(None)
        while retries:
            obj = cls(id=uuid.uuid4())
            try:
                engine.save(obj, condition=does_not_exist)
            except ConstraintViolation:
                retries -= 1
            else:
                return obj
        raise CreateFailed()
