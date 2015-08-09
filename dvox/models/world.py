import uuid
from bloop import Column, UUID, Integer, ConstraintViolation
from dvox.app import engine, config
from dvox.exceptions import CreateFailed


class World(engine.model):
    id = Column(UUID, hash_key=True)
    seed = Column(Integer, name="s")

    @classmethod
    def unique(cls, seed=0):
        retries = config["CREATE_RETRIES"]
        does_not_exist = cls.id.is_(None)
        while retries:
            obj = cls(id=uuid.uuid4(), seed=seed)
            try:
                engine.save(obj, condition=does_not_exist)
            except ConstraintViolation:
                retries -= 1
            else:
                return obj
        raise CreateFailed()
